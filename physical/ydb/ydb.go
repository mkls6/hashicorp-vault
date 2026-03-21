package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/physical"
	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const VAULT_TABLE = "vault_kv"

type YDBBackend struct {
	db     *ydb.Driver
	table  string
	logger log.Logger
}

var (
	_ physical.Backend             = (*YDBBackend)(nil)
	_ physical.Transactional       = (*YDBBackend)(nil)
	_ physical.TransactionalLimits = (*YDBBackend)(nil)
)

func NewYDBBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	// Environment variables take precedence over supplied configuration values.
	// DSN: prefer VAULT_YDB_DSN if set, otherwise use conf["dsn"].
	var dsn string
	if envDSN := os.Getenv("VAULT_YDB_DSN"); envDSN != "" {
		dsn = strings.TrimSpace(envDSN)
	} else {
		dsn = strings.TrimSpace(conf["dsn"])
		if dsn == "" {
			return &YDBBackend{}, fmt.Errorf("YDB: dsn is not set")
		}
	}

	// Table: prefer VAULT_YDB_TABLE if set, otherwise use conf["table"], falling back to default.
	var table string
	if envTable := os.Getenv("VAULT_YDB_TABLE"); envTable != "" {
		table = strings.TrimSpace(envTable)
	} else {
		table = strings.TrimSpace(conf["table"])
		if table == "" {
			table = VAULT_TABLE
		}
	}

	quotedTable, err := quoteYDBIdentifier(table)
	if err != nil {
		return &YDBBackend{}, fmt.Errorf("YDB: invalid table name: %w", err)
	}

	opts := getYDBOptionsFromConfMap(conf)

	ctx := context.TODO()
	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		errStr := "YDB: failed to open database connection"
		logger.Error(errStr, "error", err)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	// Ensure the table exists or create schema as required.
	if err = ensureTableExists(ctx, db, table, logger); err != nil {
		errStr := "YDB: failed to ensure table exists"
		logger.Error(errStr, "table", table, "error", err)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	return &YDBBackend{
		db:     db,
		table:  quotedTable,
		logger: logger,
	}, nil
}

func ensureTableExists(ctx context.Context, db *ydb.Driver, tableName string, logger log.Logger) error {
	var fullTableName string
	if strings.HasPrefix(tableName, "/") {
		fullTableName = tableName
	} else {
		fullTableName = db.Name() + "/" + tableName
	}

	_, err := db.Scheme().DescribePath(ctx, fullTableName)
	tableExists := err == nil

	if tableExists {
		logger.Info("YDB: table already exists", "table", tableName)
		return nil
	}

	logger.Info("YDB: creating table", "table", tableName)

	quotedTableName, err := quoteYDBIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("invalid table name %q: %w", tableName, err)
	}

	queryStmt := fmt.Sprintf(`
		CREATE TABLE %s (
			key Text NOT NULL,
			value Bytes,
			updated_at Timestamp,
			PRIMARY KEY (key)
		)`, quotedTableName)

	err = db.Query().Exec(ctx, queryStmt)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	logger.Info("YDB: table created successfully", "table", tableName)
	return nil
}

func (y *YDBBackend) Put(ctx context.Context, entry *physical.Entry) error {
	stmt := fmt.Sprintf("UPSERT INTO %s (key, value) VALUES ($key, $value)", y.table)
	err := y.db.Query().Exec(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(entry.Key).
				Param("$value").Bytes(entry.Value).Build()),
	)
	if err != nil {
		return fmt.Errorf("YDB: failed to put entry: "+entry.Key+" %w", err)
	}
	return nil
}

func (y *YDBBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	stmt := fmt.Sprintf("SELECT key AS Key, value AS Value FROM %s WHERE key = $key", y.table)
	q, err := y.db.Query().QueryRow(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(key).Build()),
	)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("YDB: failed to get key "+key+" %w", err)
	}

	entry := physical.Entry{}
	if err = q.ScanStruct(&entry, query.WithScanStructAllowMissingColumnsFromSelect()); err != nil {
		return nil, fmt.Errorf("YDB: failed to get key "+key+" %w", err)
	}

	return &entry, nil
}

func (y *YDBBackend) Delete(ctx context.Context, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE key = $key", y.table)
	err := y.db.Query().Exec(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(key).Build()),
	)
	if err != nil {
		return fmt.Errorf("YDB: failed to drop entry with key "+key+" %w", err)
	}
	return nil
}

func (y *YDBBackend) List(ctx context.Context, prefix string) ([]string, error) {
	errStr := "YDB: failed to list keys by prefix " + prefix
	likePrefix := prefix + "%"
	if prefix == "" {
		likePrefix = "%"
	}

	stmt := fmt.Sprintf("SELECT key FROM %s WHERE key LIKE $prefix ORDER BY key", y.table)
	q, err := y.db.Query().Query(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$prefix").Text(likePrefix).Build()),
	)
	if err != nil {
		return nil, fmt.Errorf(errStr+" %w", err)
	}
	defer q.Close(ctx)

	seen := make(map[string]struct{})
	for rs, rerr := range q.ResultSets(ctx) {
		if rerr != nil {
			return nil, fmt.Errorf(errStr+" %w", rerr)
		}
		for row, rerr := range rs.Rows(ctx) {
			if rerr != nil {
				return nil, fmt.Errorf(errStr+" %w", rerr)
			}

			var val string
			err = row.Scan(&val)
			if err != nil {
				return []string{}, fmt.Errorf("YDB: failed to list keys: %w", err)
			}

			rel := val
			if prefix != "" {
				rel = strings.TrimPrefix(val, prefix)
			}

			if rel == "" {
				continue
			}

			if idx := strings.Index(rel, "/"); idx != -1 {
				dir := rel[:idx+1]
				seen[dir] = struct{}{}
			} else {
				seen[rel] = struct{}{}
			}
		}
	}

	lst := make([]string, 0, len(seen))
	for k := range seen {
		lst = append(lst, k)
	}
	return lst, nil
}

func (y *YDBBackend) GetInternal(ctx context.Context, key string) (*physical.Entry, error) {
	return y.Get(ctx, key)
}

func (y *YDBBackend) PutInternal(ctx context.Context, entry *physical.Entry) error {
	return y.Put(ctx, entry)
}

func (y *YDBBackend) DeleteInternal(ctx context.Context, key string) error {
	return y.Delete(ctx, key)
}

type ydbTxWrapper struct {
	tx    query.TxActor
	table string
}

func (w *ydbTxWrapper) GetInternal(ctx context.Context, key string) (*physical.Entry, error) {
	stmt := fmt.Sprintf("SELECT key, value FROM %s WHERE key = $key", w.table)
	params := ydb.ParamsBuilder().Param("$key").Text(key).Build()

	res, err := w.tx.Query(ctx, stmt, query.WithParameters(params))
	if err != nil {
		return nil, err
	}
	defer res.Close(ctx)

	for rs, rerr := range res.ResultSets(ctx) {
		if rerr != nil {
			return nil, rerr
		}
		for row, rerr := range rs.Rows(ctx) {
			if rerr != nil {
				return nil, rerr
			}
			var k string
			var v []byte
			if err := row.Scan(&k, &v); err != nil {
				return nil, err
			}
			return &physical.Entry{Key: k, Value: v}, nil
		}
	}
	return nil, nil
}

func (w *ydbTxWrapper) PutInternal(ctx context.Context, entry *physical.Entry) error {
	stmt := fmt.Sprintf("UPSERT INTO %s (key, value) VALUES ($key, $value)", w.table)
	params := ydb.ParamsBuilder().
		Param("$key").Text(entry.Key).
		Param("$value").Bytes(entry.Value).Build()
	err := w.tx.Exec(ctx, stmt, query.WithParameters(params))
	return err
}

func (w *ydbTxWrapper) DeleteInternal(ctx context.Context, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE key = $key", w.table)
	params := ydb.ParamsBuilder().Param("$key").Text(key).Build()
	err := w.tx.Exec(ctx, stmt, query.WithParameters(params))
	return err
}

func (y *YDBBackend) Transaction(ctx context.Context, txns []*physical.TxnEntry) error {
	if len(txns) == 0 {
		return nil
	}
	return y.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		w := &ydbTxWrapper{tx: tx, table: y.table}
		return physical.GenericTransactionHandler(ctx, w, txns)
	})
}

// Return default transaction limits.
// See `sdk/physical/transactions.go`
func (y *YDBBackend) TransactionLimits() (int, int) {
	return 63, 128 * 1024
}

func quoteYDBIdentifier(identifier string) (string, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", fmt.Errorf("missing identifier")
	}

	hasLeadingSlash := strings.HasPrefix(identifier, "/")
	segments := strings.Split(identifier, "/")
	quotedSegments := make([]string, 0, len(segments))

	for idx, segment := range segments {
		if segment == "" {
			if !(hasLeadingSlash && idx == 0) {
				return "", fmt.Errorf("empty identifier segment")
			}
			continue
		}
		if err := validateYDBIdentifierSegment(segment); err != nil {
			return "", err
		}
		quotedSegments = append(quotedSegments, "`"+strings.ReplaceAll(segment, "`", "``")+"`")
	}

	if len(quotedSegments) == 0 {
		return "", fmt.Errorf("missing identifier")
	}

	quoted := strings.Join(quotedSegments, "/")
	if hasLeadingSlash {
		return "/" + quoted, nil
	}
	return quoted, nil
}

func validateYDBIdentifierSegment(segment string) error {
	if segment == "" {
		return fmt.Errorf("empty identifier segment")
	}
	if segment == "." || segment == ".." {
		return fmt.Errorf("reserved identifier segment %q", segment)
	}

	for _, r := range segment {
		if r == 0 {
			return fmt.Errorf("identifier segment %q contains NUL", segment)
		}
		if !unicode.IsPrint(r) {
			return fmt.Errorf("identifier segment %q contains non-printable characters", segment)
		}
	}

	return nil
}

func getYDBOptionsFromConfMap(conf map[string]string) []ydb.Option {
	var opts []ydb.Option

	// internal_ca: environment variable VAULT_YDB_INTERNAL_CA takes precedence
	internalCAVal := ""
	if envv := os.Getenv("VAULT_YDB_INTERNAL_CA"); envv != "" {
		internalCAVal = envv
	} else if v, ok := conf["internal_ca"]; ok {
		internalCAVal = v
	}
	internalCA := false
	if internalCAVal != "" && (strings.EqualFold(internalCAVal, "true") || internalCAVal == "1" || strings.EqualFold(internalCAVal, "yes")) {
		internalCA = true
	}

	// If YC-specific TLS options are set, use the ydb-go-yc helper to configure them.
	if internalCA {
		opts = append(opts, yc.WithInternalCA())
	}
	if authOpt := getYDBAuthOptionFromConfMap(conf); authOpt != nil {
		opts = append(opts, authOpt)
	}

	return opts
}

func getYDBAuthOptionFromConfMap(conf map[string]string) ydb.Option {
	switch auth := resolveYDBAuth(conf); auth.kind {
	case "token":
		return ydb.WithAccessTokenCredentials(auth.value)
	case "service_account_key_file":
		return yc.WithServiceAccountKeyFileCredentials(auth.value)
	case "service_account_key":
		return yc.WithServiceAccountKeyCredentials(auth.value)
	case "static":
		return ydb.WithStaticCredentials(auth.value, auth.value2)
	case "metadata":
		return yc.WithMetadataCredentials()
	case "anonymous":
		return ydb.WithAnonymousCredentials()
	case "environ":
		return env.WithEnvironCredentials()
	default:
		return nil
	}
}

type ydbAuthConfig struct {
	kind   string
	value  string
	value2 string
}

func resolveYDBAuth(conf map[string]string) ydbAuthConfig {
	if value := lookupFirstNonEmpty("VAULT_YDB_TOKEN", conf["token"]); value != "" {
		return ydbAuthConfig{kind: "token", value: value}
	}
	if value := lookupFirstNonEmpty("VAULT_YDB_SA_KEYFILE", conf["service_account_key_file"]); value != "" {
		return ydbAuthConfig{kind: "service_account_key_file", value: value}
	}
	if value := lookupFirstNonEmpty("VAULT_YDB_SA_KEY", conf["service_account_key"]); value != "" {
		return ydbAuthConfig{kind: "service_account_key", value: value}
	}
	if user, password := lookupStaticCredentials(conf); user != "" && password != "" {
		return ydbAuthConfig{kind: "static", value: user, value2: password}
	}
	if lookupFirstBool("VAULT_YDB_METADATA_AUTH", conf["metadata_auth"]) {
		return ydbAuthConfig{kind: "metadata"}
	}
	if lookupFirstBool("VAULT_YDB_ANONYMOUS_CREDENTIALS", conf["anonymous_credentials"]) {
		return ydbAuthConfig{kind: "anonymous"}
	}
	if hasYDBEnvironCredentials() {
		return ydbAuthConfig{kind: "environ"}
	}
	return ydbAuthConfig{}
}

func lookupStaticCredentials(conf map[string]string) (string, string) {
	user := lookupFirstNonEmpty("VAULT_YDB_STATIC_CREDENTIALS_USER", conf["static_credentials_user"])
	password := lookupFirstNonEmpty("VAULT_YDB_STATIC_CREDENTIALS_PASSWORD", conf["static_credentials_password"])
	return user, password
}

func hasYDBEnvironCredentials() bool {
	if lookupEnvNonEmpty("YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS") != "" {
		return true
	}
	if lookupEnvNonEmpty("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS") != "" {
		return true
	}
	if lookupEnvBool("YDB_METADATA_CREDENTIALS") {
		return true
	}
	if lookupEnvNonEmpty("YDB_ACCESS_TOKEN_CREDENTIALS") != "" {
		return true
	}
	if lookupEnvNonEmpty("YDB_STATIC_CREDENTIALS_USER") != "" &&
		lookupEnvNonEmpty("YDB_STATIC_CREDENTIALS_PASSWORD") != "" &&
		lookupEnvNonEmpty("YDB_STATIC_CREDENTIALS_ENDPOINT") != "" {
		return true
	}
	if lookupEnvNonEmpty("YDB_OAUTH2_KEY_FILE") != "" {
		return true
	}
	if value, ok := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); ok && strings.TrimSpace(value) == "0" {
		return true
	}
	return false
}

func lookupFirstNonEmpty(envKey, confValue string) string {
	if envv := strings.TrimSpace(os.Getenv(envKey)); envv != "" {
		return envv
	}
	return strings.TrimSpace(confValue)
}

func lookupFirstBool(envKey, confValue string) bool {
	if envv, ok := os.LookupEnv(envKey); ok {
		if strings.TrimSpace(envv) == "" {
			return parseYDBBool(confValue)
		}
		return parseYDBBool(envv)
	}
	return parseYDBBool(confValue)
}

func lookupEnvNonEmpty(envKey string) string {
	return strings.TrimSpace(os.Getenv(envKey))
}

func lookupEnvBool(envKey string) bool {
	envv, ok := os.LookupEnv(envKey)
	if !ok {
		return false
	}
	return parseYDBBool(envv)
}

func parseYDBBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}
