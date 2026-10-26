package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// The CLI speaks the same private Unix protocol as the hourly scheduler. It
// never opens a database and exposes no SQL or replacement-body option.
func newHistoryMaintenanceCommand() *cobra.Command {
	var path, stage, cursor, cutoff string
	var device, inode uint64
	var timeout time.Duration
	cmd := &cobra.Command{Use: "history-maintenance", Short: "Run one typed batch through the private AO maintenance socket", Args: usageArgs(cobra.NoArgs), RunE: func(cmd *cobra.Command, args []string) error {
		if path == "" || stage == "" || cutoff == "" || device == 0 || inode == 0 || timeout <= 0 {
			return usageError{fmt.Errorf("socket, stage, cutoff, database-device and database-inode are required")}
		}
		transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}}
		defer transport.CloseIdleConnections()
		raw, err := json.Marshal(map[string]any{"stage": stage, "cursor": cursor, "cutoff": cutoff, "database_device": device, "database_inode": inode})
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(cmd.Context(), http.MethodPost, "http://localhost/v1/history/batch", strings.NewReader(string(raw)))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		client := http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("private maintenance unavailable")
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
		if err != nil || len(body) > 65536 || !json.Valid(body) {
			return fmt.Errorf("invalid private maintenance response")
		}
		if response.StatusCode != 200 {
			return fmt.Errorf("private maintenance failed (%d)", response.StatusCode)
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), string(body))
		return err
	}}
	cmd.Flags().StringVar(&path, "socket", os.Getenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET"), "private Unix socket")
	cmd.Flags().StringVar(&stage, "stage", "", "typed retention stage")
	cmd.Flags().StringVar(&cursor, "cursor", "", "previous confirmed PK cursor")
	cmd.Flags().StringVar(&cutoff, "cutoff", "", "UTC cutoff; cannot exceed daemon retention policy")
	cmd.Flags().Uint64Var(&device, "database-device", 0, "expected database device")
	cmd.Flags().Uint64Var(&inode, "database-inode", 0, "expected database inode")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "bounded request timeout")
	return cmd
}
