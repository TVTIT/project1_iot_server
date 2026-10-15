// Command sqlite-spike checks one SQLite commit and reopen; it is not an outbox.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func run() error {
	dir, err := os.MkdirTemp("", "task30-sqlite-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "probe.sqlite")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if _, err = db.ExecContext(ctx, "CREATE TABLE probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO probe (id,value) VALUES (?,?)", 1, "committed"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	reopened, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	reopened.SetMaxOpenConns(1)
	defer reopened.Close()
	var value, version string
	var count int
	if err = reopened.QueryRowContext(ctx, "SELECT count(*) FROM probe").Scan(&count); err != nil {
		return err
	}
	if err = reopened.QueryRowContext(ctx, "SELECT value FROM probe WHERE id=?", 1).Scan(&value); err != nil {
		return err
	}
	if err = reopened.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return err
	}
	if count != 1 || value != "committed" {
		return fmt.Errorf("unexpected persisted row: count=%d", count)
	}
	fmt.Printf("PASS: SQLite %s commit, close, reopen, exact row verified\n", version)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "SQLite spike failed:", err)
		os.Exit(1)
	}
}
