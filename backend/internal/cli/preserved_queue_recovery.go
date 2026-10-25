package cli

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
	"os"
)

func newSessionRecoverPreservedQueueCommand(c *commandContext) *cobra.Command {
	var request string
	cmd := &cobra.Command{Use: "recover-preserved-queue <session-id>", Short: "Recover an explicitly proven original queue through local maintenance API", Args: usageArgs(cobra.ExactArgs(1)), RunE: func(cmd *cobra.Command, args []string) error {
		if request == "" {
			return usageError{fmt.Errorf("--request is required")}
		}
		info, err := os.Lstat(request)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 1<<20 || info.Mode().Perm()&0077 != 0 {
			return usageError{fmt.Errorf("request must be a private regular metadata file (maximum 1MiB)")}
		}
		raw, err := os.ReadFile(request)
		if err != nil {
			return err
		}
		if !json.Valid(raw) {
			return usageError{fmt.Errorf("request must be valid JSON")}
		}
		var out struct {
			Restored           int    `json:"restored"`
			PreviouslyRestored int    `json:"previouslyRestored"`
			RequestSHA256      string `json:"requestSha256"`
			Dispatch           string `json:"dispatch"`
		}
		if err = c.postJSON(cmd.Context(), "dev/sessions/"+url.PathEscape(args[0])+"/preserved-queue/recover", json.RawMessage(raw), &out); err != nil {
			return err
		}
		return writeJSON(cmd.OutOrStdout(), out)
	}}
	cmd.Flags().StringVar(&request, "request", "", "private JSON metadata request; no prompt bodies")
	return cmd
}
