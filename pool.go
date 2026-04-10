package pgdb

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgDBConfig struct {
	Host                string `yaml:"Host"`
	Port                int    `yaml:"Port"`
	User                string `yaml:"User"`
	Password            string `yaml:"Password"`
	DBName              string `yaml:"DBName"`
	MaxConns            int    `yaml:"MaxConns"`
	MinConns            int    `yaml:"MinConns"`
	ConnMaxLifetime     int    `yaml:"ConnMaxLifetime"`
	ConnMaxIdleTime     int    `yaml:"ConnMaxIdleTime"`
	HealthCheckInterval int    `yaml:"HealthCheckInterval"`
	ExecTimeout         int    `yaml:"ExecTimeout"`
	QueryTimeout        int    `yaml:"QueryTimeout"`
	SSLMode             string `yaml:"SSLMode"`
}

func NewPgDBConfig() *PgDBConfig {
	return &PgDBConfig{}
}

type poolIface interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

type PgxDB struct {
	Pool         poolIface
	execTimeout  time.Duration
	queryTimeout time.Duration
}

func NewPgxDB(cfg *PgDBConfig) (*PgxDB, error) {
	// 密码 URL 编码防特殊字符注入
	encodedPassword := url.QueryEscape(cfg.Password)

	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v",
		cfg.User, encodedPassword, cfg.Host, cfg.Port, cfg.DBName, cfg.SSLMode)

	pgcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database configuration: %w", err)
	}

	pgcfg.MaxConns = int32(cfg.MaxConns)
	pgcfg.MinConns = int32(cfg.MinConns)
	pgcfg.MaxConnLifetime = time.Duration(cfg.ConnMaxLifetime) * time.Second
	pgcfg.MaxConnIdleTime = time.Duration(cfg.ConnMaxIdleTime) * time.Second
	pgcfg.HealthCheckPeriod = time.Duration(cfg.HealthCheckInterval) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, pgcfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgxpool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PgxDB{
		Pool:         pool,
		execTimeout:  time.Duration(cfg.ExecTimeout) * time.Second,
		queryTimeout: time.Duration(cfg.QueryTimeout) * time.Second,
	}, nil
}

// 内部工具：生成带超时的 context
func (db *PgxDB) getCtxWithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {} // 返回一个空操作的 cancel
}

func (db *PgxDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	ctx, cancel := db.getCtxWithTimeout(ctx, db.execTimeout)
	defer cancel()

	ct, err := db.Pool.Exec(ctx, sql, args...)
	return ct.RowsAffected(), err
}

func (db *PgxDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, cancel := db.getCtxWithTimeout(ctx, db.queryTimeout)
	defer cancel() // 注意：pgx.Row 会在 Scan() 时执行查询，这里的 defer 取决于底层实现，pgx5 中是安全的
	return db.Pool.QueryRow(ctx, sql, args...)
}

func (db *PgxDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, cancel := db.getCtxWithTimeout(ctx, db.queryTimeout)
	defer cancel()

	return db.Pool.Query(ctx, sql, args...)
}

func (db *PgxDB) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	ctx, cancel := db.getCtxWithTimeout(ctx, db.execTimeout)
	defer cancel()

	return db.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (db *PgxDB) WithTx(ctx context.Context, fn func(tx Tx) error) error {
	// 直接通过 Pool 开启事务
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	wrappedTx := &pgxTx{tx: tx, db: db}

	// 执行业务逻辑
	if err := fn(wrappedTx); err != nil {
		return err // 业务出错直接返回，defer 会自动触发回滚
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}
	return nil
}

func (db *PgxDB) Ping(ctx context.Context) error {
	return db.Pool.Ping(ctx)
}

func (db *PgxDB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}

// pgxTx 实现 Tx 接口
type pgxTx struct {
	tx pgx.Tx
	db *PgxDB // 引入 db 以复用超时配置
}

func (t *pgxTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	ctx, cancel := t.db.getCtxWithTimeout(ctx, t.db.execTimeout)
	defer cancel()

	ct, err := t.tx.Exec(ctx, sql, args...)
	return ct.RowsAffected(), err
}

func (t *pgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, cancel := t.db.getCtxWithTimeout(ctx, t.db.queryTimeout)
	defer cancel()

	return t.tx.QueryRow(ctx, sql, args...)
}

func (t *pgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, cancel := t.db.getCtxWithTimeout(ctx, t.db.queryTimeout)
	defer cancel()

	return t.tx.Query(ctx, sql, args...)
}

func (t *pgxTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	ctx, cancel := t.db.getCtxWithTimeout(ctx, t.db.execTimeout)
	defer cancel()

	return t.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}
