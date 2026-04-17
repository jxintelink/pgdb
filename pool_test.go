package pgdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v3"
)

// 1. 测试数据库初始化与 Ping
func TestPgxDB_NewAndPing(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	// 模拟 Ping 成功
	mock.ExpectPing()

	db := &PgxDB{Pool: mock}
	err = db.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

// 2. 测试 Query 逻辑与超时上下文
func TestPgxDB_QueryWithTimeout(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	// 设置一个极短的超时用于测试
	db := &PgxDB{
		Pool:         mock,
		queryTimeout: 10 * time.Millisecond,
	}

	sql := "SELECT id FROM t_cdrs WHERE calling_number = \\$1"

	// 场景 A: 正常查询
	mock.ExpectQuery(sql).
		WithArgs("123456").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))

	rows, err := db.Query(context.Background(), "SELECT id FROM t_cdrs WHERE calling_number = $1", "123456")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	rows.Close()

	// 场景 B: 模拟超时 (利用 WillDelay 让查询超过 10ms)
	mock.ExpectQuery(sql).
		WithArgs("超时测试").
		WillReturnError(context.DeadlineExceeded)

	_, err = db.Query(context.Background(), "SELECT id FROM t_cdrs WHERE calling_number = $1", "超时测试")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

// 3. 完整的事务流测试 (Commit & Rollback)
func TestPgxDB_WithTx_Full(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	db := &PgxDB{Pool: mock}

	t.Run("SuccessfulTransaction", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO t_cdrs").WillReturnResult(pgxmock.NewResult("INSERT", 1))
		mock.ExpectExec("UPDATE logs").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		mock.ExpectCommit()

		err := db.WithTx(context.Background(), func(tx Tx) error {
			if _, err := tx.Exec(context.Background(), "INSERT INTO t_cdrs..."); err != nil {
				return err
			}
			if _, err := tx.Exec(context.Background(), "UPDATE logs..."); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Errorf("Expected success, got: %v", err)
		}
	})

	t.Run("FailedTransaction_ManualRollback", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO t_cdrs").WillReturnError(errors.New("network error"))
		mock.ExpectRollback()

		err := db.WithTx(context.Background(), func(tx Tx) error {
			_, err := tx.Exec(context.Background(), "INSERT INTO t_cdrs...")
			return err // 返回错误应触发 defer 中的 Rollback
		})
		if err == nil {
			t.Error("Expected error but got nil")
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

// 4. 高性能批量导入测试 (CopyFrom)
func TestPgxDB_CopyFrom(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	db := &PgxDB{Pool: mock}

	targetTable := pgx.Identifier{"t_cdrs_20260410"}
	columns := []string{"id", "duration", "status"}

	mock.ExpectCopyFrom(targetTable, columns).WillReturnResult(100)

	// 模拟数据源
	rows := [][]any{
		{1, 30, "completed"},
		{2, 45, "failed"},
	}

	count, err := db.CopyFrom(
		context.Background(),
		targetTable,
		columns,
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		t.Errorf("CopyFrom failed: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100 rows copied, got %d", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

// 5. 连接池释放测试
func TestPgxDB_Close(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}

	db := &PgxDB{Pool: mock}

	// 预期连接池关闭
	mock.ExpectClose()
	db.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

type noAcquirePoolStub struct {
	execCalled bool
}

func (s *noAcquirePoolStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	s.execCalled = true
	return pgconn.NewCommandTag("INSERT 1"), nil
}

func (s *noAcquirePoolStub) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (s *noAcquirePoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (s *noAcquirePoolStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (s *noAcquirePoolStub) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("not implemented")
}

func (s *noAcquirePoolStub) Ping(context.Context) error {
	return nil
}

func (s *noAcquirePoolStub) Close() {}

type acquireErrPoolStub struct {
	noAcquirePoolStub
}

func (s *acquireErrPoolStub) Acquire(context.Context) (*pgxpool.Conn, error) {
	return nil, errors.New("acquire failed")
}

// 6. 非池化场景下也能获取/释放抽象连接
func TestPgxDB_AcquireReleaseConn_Fallback(t *testing.T) {
	pool := &noAcquirePoolStub{}
	db := &PgxDB{Pool: pool}

	conn, err := db.AcquireConn(context.Background())
	if err != nil {
		t.Fatalf("AcquireConn failed: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	affected, err := conn.Exec(context.Background(), "INSERT INTO t_cdrs VALUES (1)")
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected affected rows 1, got %d", affected)
	}
	if !pool.execCalled {
		t.Fatal("expected Exec to delegate to underlying pool iface")
	}

	// fallback 连接 Release 为 no-op，不应 panic。
	db.ReleaseConn(conn)
	db.ReleaseConn(nil)
}

// 7. 池化实现获取失败时应返回错误
func TestPgxDB_AcquireConn_Error(t *testing.T) {
	db := &PgxDB{Pool: &acquireErrPoolStub{}}

	conn, err := db.AcquireConn(context.Background())
	if err == nil {
		t.Fatal("expected acquire error but got nil")
	}
	if conn != nil {
		t.Fatal("expected nil connection on acquire error")
	}
}
