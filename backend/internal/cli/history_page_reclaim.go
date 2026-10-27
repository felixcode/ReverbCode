package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// This one-shot client never opens SQLite and has no arbitrary SQL/path/table.
func newHistoryPageReclaimCommand() *cobra.Command {
	var socket string
	var device, inode uint64
	var pages, free int64
	var writeBudget, timeout time.Duration
	cmd := &cobra.Command{Use: "history-page-reclaim", Short: "Request one bounded free-page batch through AO's private Unix socket", Args: usageArgs(cobra.NoArgs), RunE: func(cmd *cobra.Command, _ []string) error {
		if socket == "" || device == 0 || inode == 0 || pages < 0 || free < 0 || writeBudget < 0 || timeout <= 0 {
			return usageError{fmt.Errorf("private socket and database identity are required; limits must be nonnegative")}
		}
		transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		defer transport.CloseIdleConnections()
		body, err := json.Marshal(map[string]any{"database_device": device, "database_inode": inode, "max_pages": pages, "min_free_pages": free, "write_budget_ns": writeBudget.Nanoseconds()})
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(cmd.Context(), http.MethodPost, "http://localhost/v1/history/reclaim", bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		client := http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("private reclaim response unavailable; commit outcome may be unknown")
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
		if err != nil || len(data) > 65536 || !json.Valid(data) {
			return fmt.Errorf("invalid private reclaim response")
		}
		var raw map[string]json.RawMessage
		if err = json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("invalid private reclaim result")
		}
		safe := map[string]json.RawMessage{}
		for _, key := range []string{"status", "reason", "commit_state", "transaction_committed", "attempted_steps", "freelist_pages_removed", "logical_pages_removed", "logical_page_bytes_removed", "before", "after", "files_before", "files_after", "sqlite_version", "connection_restore_failed", "connection_discarded", "autocheckpoint_restore_failed", "writer_reserved_ms", "held_ms", "commit_ms", "connection_release_ms", "error_type", "sqlite_code"} {
			if v, ok := raw[key]; ok {
				safe[key] = v
			}
		}
		if err = writeJSON(cmd.OutOrStdout(), safe); err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("private reclaim failed (%d); inspect commit_state", response.StatusCode)
		}
		return nil
	}}
	cmd.Flags().StringVar(&socket, "socket", os.Getenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET"), "private Unix socket")
	cmd.Flags().Uint64Var(&device, "database-device", 0, "expected own database device")
	cmd.Flags().Uint64Var(&inode, "database-inode", 0, "expected own database inode")
	cmd.Flags().Int64Var(&pages, "max-pages", 0, "lower daemon step ceiling; zero uses configured value")
	cmd.Flags().Int64Var(&free, "min-free-pages", 0, "raise daemon free-page threshold; zero uses configured value")
	cmd.Flags().DurationVar(&writeBudget, "write-budget", 0, "lower daemon write budget; zero uses configured value")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "one-shot request timeout")
	return cmd
}
