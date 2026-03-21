package ydb

import (
	"fmt"
	"testing"

	log "github.com/hashicorp/go-hclog"
	helper "github.com/hashicorp/vault/helper/testhelpers/ydb"
	"github.com/hashicorp/vault/sdk/helper/logging"
	"github.com/hashicorp/vault/sdk/physical"
)

func TestYDBBackend(t *testing.T) {
	logger := logging.NewVaultLogger(log.Debug)

	cleanup, cfg := helper.PrepareTestContainer(t)
	defer cleanup()

	logger.Info(fmt.Sprintf("YDB DSN: %v", cfg.DSN))
	logger.Info(fmt.Sprintf("YDB VAULT TABLE: %v", cfg.Table))

	backend, err := NewYDBBackend(map[string]string{
		"dsn":   cfg.DSN,
		"table": cfg.Table,
	}, logger)
	if err != nil {
		t.Fatalf("Failed to create new backend: %v", err)
	}

	logger.Info("Running basic backend tests")
	physical.ExerciseBackend(t, backend)

	logger.Info("Running list prefix backend tests")
	physical.ExerciseBackend_ListPrefix(t, backend)
	physical.ExerciseTransactionalBackend(t, backend)
}

func TestQuoteYDBIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		expectErr bool
	}{
		{
			name:  "simple table",
			input: "vault_kv",
			want:  "`vault_kv`",
		},
		{
			name:  "absolute path",
			input: "/local/vault_kv",
			want:  "/`local`/`vault_kv`",
		},
		{
			name:  "escapes backticks",
			input: "vault`kv",
			want:  "`vault``kv`",
		},
		{
			name:      "rejects reserved segment",
			input:     "/local/../vault_kv",
			expectErr: true,
		},
		{
			name:      "rejects empty middle segment",
			input:     "local//vault_kv",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := quoteYDBIdentifier(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("quoteYDBIdentifier(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
