package cmd

import (
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var setAliasCmd = &cobra.Command{
	Use:   "setAlias Alias Index",
	Short: "设置别名",
	Long:  `清空当前Alias与所有Index的关联, 并重新将Alias关联到当前Index`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.SetAlias(args[0], args[1])
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Println("设置成功")
	},
	PreRun: Connect,
}

func init() {
	rootCmd.AddCommand(setAliasCmd)
}
