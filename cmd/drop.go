package cmd

import (
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var dropCmd = &cobra.Command{
	Use:   "drop Index",
	Short: "删除索引",
	Long:  `删除索引`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.Drop(args[0])
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Println("成功删除索引")
	},
	PreRun: Connect,
}

func init() {
	rootCmd.AddCommand(dropCmd)
}
