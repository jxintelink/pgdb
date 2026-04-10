package pgdb

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// 新增：支持 PostgreSQL 的原生 COPY 协议，非常适合批量写入每天的 t_cdrs 分区表
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	WithTx(ctx context.Context, fn func(tx Tx) error) error
	Ping(ctx context.Context) error
	Close()
}

type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}
