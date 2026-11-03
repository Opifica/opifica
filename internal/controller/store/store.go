// Package store is the controller's PostgreSQL storage. Queries are written
// in queries/*.sql and compiled to Go by sqlc (package db); the schema lives
// in migrations/*.sql and is applied at startup.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/opifica/opifica/internal/controller/store/db"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Store wraps a connection pool and the generated queries.
type Store struct {
	Pool *pgxpool.Pool
	*db.Queries
}

// Open connects to PostgreSQL and applies pending migrations.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to database: %w", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool, Queries: db.New(pool)}, nil
}

// Close closes the connection pool.
func (s *Store) Close() { s.Pool.Close() }

// Tx runs fn in a transaction, committing if it returns nil.
func (s *Store) Tx(ctx context.Context, fn func(q *db.Queries) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		return fn(s.Queries.WithTx(tx))
	})
}

// IsNotFound reports whether err means a query returned no row.
func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	var ms []migration
	for _, e := range entries {
		num, _, ok := strings.Cut(e.Name(), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil {
			return nil, fmt.Errorf("migration %q: name must start with a number, e.g. 0002_runs.sql", e.Name())
		}
		body, err := migrationFiles.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{version: v, name: e.Name(), sql: string(body)})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	for i := 1; i < len(ms); i++ {
		if ms[i].version == ms[i-1].version {
			return nil, fmt.Errorf("migrations %q and %q have the same number", ms[i-1].name, ms[i].name)
		}
	}
	return ms, nil
}

// migrationLock is a PostgreSQL advisory lock key, so controllers starting at
// the same time don't apply migrations twice.
const migrationLock = 0x6f70696669636121 // "opifica!"

// Migrate applies every migration not yet recorded in schema_migrations, each
// in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(migrationLock)); err != nil {
		return fmt.Errorf("taking migration lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", int64(migrationLock))

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}

	applied := map[int]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range ms {
		if applied[m.version] {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name)
			return err
		})
		if err != nil {
			return fmt.Errorf("applying migration %s: %w", m.name, err)
		}
	}
	return nil
}
