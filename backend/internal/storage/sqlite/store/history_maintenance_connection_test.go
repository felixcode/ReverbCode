package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type maintenanceFaultDriver struct {
	cancel      context.CancelFunc
	mu          sync.Mutex
	connections []*maintenanceFaultConn
}

func (d *maintenanceFaultDriver) Open(string) (driver.Conn, error) {
	c := &maintenanceFaultConn{driver: d, busy: 5000}
	d.mu.Lock()
	d.connections = append(d.connections, c)
	d.mu.Unlock()
	return c, nil
}
func (d *maintenanceFaultDriver) Connect(context.Context) (driver.Conn, error) { return d.Open("") }
func (d *maintenanceFaultDriver) Driver() driver.Driver                        { return d }

type maintenanceFaultConn struct {
	driver *maintenanceFaultDriver
	busy   int
	closed bool
}

func (c *maintenanceFaultConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *maintenanceFaultConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c *maintenanceFaultConn) Close() error                        { c.closed = true; return nil }
func (c *maintenanceFaultConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &maintenanceFaultRows{value: int64(c.busy)}, nil
}
func (c *maintenanceFaultConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	value, err := strconv.Atoi(strings.TrimPrefix(query, "PRAGMA busy_timeout="))
	if err != nil {
		return nil, err
	}
	if value != 0 && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	c.busy = value
	if value == 0 {
		if c.driver.cancel != nil {
			c.driver.cancel()
		}
		return nil, context.Canceled
	}
	return driver.RowsAffected(0), nil
}

type maintenanceFaultRows struct {
	value int64
	done  bool
}

func (*maintenanceFaultRows) Columns() []string { return []string{"timeout"} }
func (*maintenanceFaultRows) Close() error      { return nil }
func (r *maintenanceFaultRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	values[0] = r.value
	r.done = true
	return nil
}

func TestMaintenanceAppliedPragmaFailureRestoresOrDiscards(t *testing.T) {
	for _, cancelContext := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelContext), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := &maintenanceFaultDriver{}
			if cancelContext {
				d.cancel = cancel
			}
			db := sql.OpenDB(d)
			db.SetMaxOpenConns(1)
			defer db.Close()
			connection, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			result := HistoryMaintenanceResult{Retained: map[string]int{}}
			err = withMaintenanceConnection(ctx, connection, &result, func() error { t.Fatal("mutation entered after uncertain PRAGMA"); return nil })
			connection.Close()
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			var busy int
			if err = db.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&busy); err != nil || busy != 5000 {
				t.Fatal("business setting leaked", busy, err)
			}
			if cancelContext && (!result.ConnectionRestoreFailed || !result.ConnectionDiscarded || !d.connections[0].closed) {
				t.Fatal("uncertain writer was not discarded", result)
			}
			if !cancelContext && (result.ConnectionRestoreFailed || d.connections[0].busy != 5000) {
				t.Fatal("restoration was not attempted", result)
			}
		})
	}
}
