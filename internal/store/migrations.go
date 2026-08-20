package store

import (
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// RunMigrations applies any unapplied *.up.sql files in order.
// Applied versions are tracked in a schema_migrations table.
func RunMigrations(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT PRIMARY KEY,
		applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		version := strings.TrimSuffix(name, ".up.sql")

		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version=?", version).Scan(&n); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if n > 0 {
			continue
		}

		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := db.Exec(string(data)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := db.Exec("INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	return nil
}
