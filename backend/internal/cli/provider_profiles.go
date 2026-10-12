package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/url"
	"os"
	"strings"
)

type providerProfileCLI struct {
	Env   map[string]string `json:"env,omitempty"`
	Label string            `json:"label,omitempty"`
	Roles []string          `json:"roles,omitempty"`
}
type providerProfilesCLIResult struct {
	ActiveProviderProfile string `json:"activeProviderProfile"`
	Profiles              []struct {
		Name    string   `json:"name"`
		Label   string   `json:"label"`
		Roles   []string `json:"roles"`
		EnvKeys []string `json:"envKeys"`
	} `json:"profiles"`
}

func newProjectProviderCommand(ctx *commandContext) *cobra.Command {
	var projectID string
	var asJSON bool
	var file string
	root := &cobra.Command{Use: "provider", Short: "Manage project provider profiles (new workers and reviewers only)"}
	root.PersistentFlags().StringVarP(&projectID, "project", "p", "", "Project ID (default AO_PROJECT_ID)")
	root.PersistentFlags().BoolVar(&asJSON, "json", false, "Output metadata as JSON")
	path := func() (string, error) {
		id := strings.TrimSpace(projectID)
		if id == "" {
			id = strings.TrimSpace(os.Getenv("AO_PROJECT_ID"))
		}
		if id == "" {
			return "", usageError{errors.New("--project is required")}
		}
		return "projects/" + url.PathEscape(id) + "/provider-profiles", nil
	}
	print := func(cmd *cobra.Command, out providerProfilesCLIResult) error {
		if asJSON {
			return writeJSON(cmd.OutOrStdout(), out)
		}
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "active: %s\n", out.ActiveProviderProfile); err != nil {
			return err
		}
		for _, p := range out.Profiles {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %s [%s]\n", p.Name, p.Label, strings.Join(p.Roles, ",")); err != nil {
				return err
			}
		}
		return nil
	}
	root.AddCommand(&cobra.Command{Use: "ls", Short: "List profiles without env values", Args: noArgs, RunE: func(cmd *cobra.Command, args []string) error {
		p, err := path()
		if err != nil {
			return err
		}
		var out providerProfilesCLIResult
		if err = ctx.getJSON(cmd.Context(), p, &out); err != nil {
			return err
		}
		return print(cmd, out)
	}})
	root.AddCommand(&cobra.Command{Use: "use <profile>", Short: "Atomically select an existing profile", Args: usageArgs(cobra.ExactArgs(1)), RunE: func(cmd *cobra.Command, args []string) error {
		p, err := path()
		if err != nil {
			return err
		}
		var out providerProfilesCLIResult
		name := strings.TrimSpace(args[0])
		if name == "" {
			return usageError{errors.New("profile name required")}
		}
		if err = ctx.patchJSON(cmd.Context(), p, struct {
			Active string `json:"activeProviderProfile"`
		}{name}, &out); err != nil {
			return err
		}
		return print(cmd, out)
	}})
	configure := &cobra.Command{Use: "configure", Short: "Patch provider profiles from a JSON file; preserve all unrelated config", Args: noArgs, RunE: func(cmd *cobra.Command, args []string) error {
		p, err := path()
		if err != nil {
			return err
		}
		if file == "" {
			return usageError{errors.New("--file is required")}
		}
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()
		var in struct {
			Profiles *map[string]providerProfileCLI `json:"providerProfiles,omitempty"`
			Active   *string                        `json:"activeProviderProfile,omitempty"`
		}
		d := json.NewDecoder(f)
		d.DisallowUnknownFields()
		if err = d.Decode(&in); err != nil {
			return usageError{errors.New("invalid provider profiles file")}
		}
		var extra any
		if err = d.Decode(&extra); err != io.EOF {
			return usageError{errors.New("provider profiles file has trailing content")}
		}
		if in.Profiles == nil && in.Active == nil {
			return usageError{errors.New("provider patch is empty")}
		}
		var out providerProfilesCLIResult
		if err = ctx.patchJSON(cmd.Context(), p, in, &out); err != nil {
			return err
		}
		return print(cmd, out)
	}}
	configure.Flags().StringVar(&file, "file", "", "JSON provider patch file")
	root.AddCommand(configure)
	return root
}
