package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_EnvironmentOverridesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("http:\n  address: 127.0.0.1:8080\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_HTTP_ADDRESS", "127.0.0.1:9090")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.Address != "127.0.0.1:9090" {
		t.Fatalf("HTTP.Address = %q, want %q", cfg.HTTP.Address, "127.0.0.1:9090")
	}
	if cfg.Retention.TaskHistory != 365*24*time.Hour || cfg.Retention.CleanupInterval != time.Hour || cfg.Retention.CleanupBatchSize != 500 {
		t.Fatalf("unexpected workflow retention defaults: %+v", cfg.Retention)
	}
	if cfg.EventBus.PublishedRetention != 14*24*time.Hour || cfg.EventBus.CleanupInterval != time.Hour || cfg.EventBus.CleanupBatchSize != 500 {
		t.Fatalf("unexpected outbox retention defaults: %+v", cfg.EventBus)
	}
}

func TestLoad_WorkflowRetentionEnvironmentOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("http:\n  address: 127.0.0.1:8080\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_RETENTION_TASK_HISTORY", "17520h")
	t.Setenv("APP_RETENTION_CLEANUP_BATCH_SIZE", "250")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Retention.TaskHistory != 730*24*time.Hour || cfg.Retention.CleanupBatchSize != 250 {
		t.Fatalf("unexpected workflow retention overrides: %+v", cfg.Retention)
	}
}

func TestConfig_ValidateJWTSecret(t *testing.T) {
	t.Parallel()
	cfg := Config{HTTP: HTTP{Address: "127.0.0.1:8080"}, Auth: Auth{ClientID: "client", ClientSecret: "secret"}, JWT: JWT{Secret: "short"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}

func TestLoadWithProfile_MergesProfileThenEnvironment(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "config.yaml")
	profile := filepath.Join(dir, "config-test.yaml")
	if err := os.WriteFile(base, []byte("app:\n  env: development\nlog:\n  level: info\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profile, []byte("log:\n  level: debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_LOG_LEVEL", "error")
	cfg, err := LoadWithProfile(base, "test")
	if err != nil {
		t.Fatalf("LoadWithProfile() error = %v", err)
	}
	if cfg.App.Env != "test" || cfg.Runtime.ActiveProfile != "test" {
		t.Fatalf("active profile = %q/%q", cfg.App.Env, cfg.Runtime.ActiveProfile)
	}
	if cfg.Log.Level != "error" {
		t.Fatalf("Log.Level = %q, want environment override", cfg.Log.Level)
	}
	if len(cfg.Runtime.ConfigFiles) != 2 || cfg.Runtime.ConfigFiles[1] != profile {
		t.Fatalf("ConfigFiles = %v", cfg.Runtime.ConfigFiles)
	}
}

func TestConfig_ValidateAuthSkipPattern(t *testing.T) {
	t.Parallel()
	cfg := Config{HTTP: HTTP{Address: "127.0.0.1:8080", RequestTimeout: time.Second}, Health: Health{DatabaseTimeout: time.Second, RedisTimeout: time.Second}, Auth: Auth{SkipHTTPPaths: []string{"/api/v1/[broken"}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid wildcard error")
	}
}

func TestConfig_ValidateAutoMigration(t *testing.T) {
	t.Parallel()
	cfg := Config{
		HTTP:      HTTP{Address: "127.0.0.1:8080", RequestTimeout: time.Second},
		Health:    Health{DatabaseTimeout: time.Second, RedisTimeout: time.Second},
		Migration: Migration{AutoUp: true, Path: "migrations/postgres"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want auto migration dependency error")
	}
}

func TestLoadWithProfile_ProductionDependencies(t *testing.T) {
	t.Setenv("APP_DATABASE_DSN", "postgres://app:secret@postgres:5432/platform?sslmode=require&search_path=workflow")
	t.Setenv("APP_MIGRATION_DATABASE_URL", "postgres://app:secret@postgres:5432/platform?sslmode=require&search_path=workflow")
	t.Setenv("APP_REDIS_ADDRESS", "redis:6379")
	t.Setenv("APP_AUTH_JWKS_URL", "http://identity-service:8080/.well-known/jwks.json")
	t.Setenv("APP_GRPC_TLS_CERT_FILE", "/etc/workflow-service/tls/tls.crt")
	t.Setenv("APP_GRPC_TLS_KEY_FILE", "/etc/workflow-service/tls/tls.key")
	t.Setenv("APP_TEMPORAL_HOST_PORT", "temporal:7233")
	t.Setenv("APP_OUTBOUND_GRPC_AUTHORIZATION_TARGET", "dns:///authorization-service.platform-production.svc.cluster.local:9090")

	cfg, err := LoadWithProfile(filepath.Join("..", "..", "config", "config.yaml"), "production")
	if err != nil {
		t.Fatalf("LoadWithProfile() error = %v", err)
	}
	upstream, ok := cfg.Outbound.GRPC["authorization"]
	if !ok || upstream.Target != "dns:///authorization-service.platform-production.svc.cluster.local:9090" {
		t.Fatalf("authorization upstream = %#v, found = %v", upstream, ok)
	}
	if !cfg.EventBus.Enabled || !cfg.Temporal.Enabled || cfg.Auth.Audience != "workflow-service" {
		t.Fatalf("production dependencies not enabled: event_bus=%v temporal=%v audience=%q", cfg.EventBus.Enabled, cfg.Temporal.Enabled, cfg.Auth.Audience)
	}
}
