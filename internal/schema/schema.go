// Package schema creates the database tables.
package schema

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var SQL string

// Apply creates the schema if it doesn't exist.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, SQL)
	return err
}
