package commands

import (
	"github.com/spf13/cobra"
)

var completionCmd = &cobra.Command{
	Use:   "completion [shell]",
	Short: "Generate shell completion scripts",
	Long: `Generate shell completion scripts for gfsctl.

Supported shells: bash, zsh, fish, powershell

To load completions:

Bash:
  # Linux
  $ gfsctl completion bash > /etc/bash_completion.d/gfsctl

  # macOS (Homebrew)
  $ gfsctl completion bash > $(brew --prefix)/etc/bash_completion.d/gfsctl

  # Current session only
  $ source <(gfsctl completion bash)

Zsh:
  # If shell completion is not already enabled in your environment,
  # you will need to enable it. You can execute the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ gfsctl completion zsh > "${fpath[1]}/_gfsctl"

  # You will need to start a new shell for this setup to take effect.

Fish:
  $ gfsctl completion fish > ~/.config/fish/completions/gfsctl.fish

  # Or for system-wide installation:
  $ gfsctl completion fish > /usr/share/fish/vendor_completions.d/gfsctl.fish

PowerShell:
  PS> gfsctl completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, add the output of the above
  # command to your PowerShell profile.`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return rootCmd.GenBashCompletion(cmd.OutOrStdout())
		case "zsh":
			return rootCmd.GenZshCompletion(cmd.OutOrStdout())
		case "fish":
			return rootCmd.GenFishCompletion(cmd.OutOrStdout(), true)
		case "powershell":
			return rootCmd.GenPowerShellCompletionWithDesc(cmd.OutOrStdout())
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(completionCmd)
}
