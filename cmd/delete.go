package cmd

import (
	"esync/dblayer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete Index Type Key Value",
	Short: "删除数据",
	Long:  `根据输入的key与value, 删除对应的数据`,
	Args:  cobra.ExactArgs(4),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.Delete(&dblayer.DeleteRequest{
			Index: args[0],
			Type:  args[1],
			Key:   args[2],
			Value: args[3],
		})
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Printf("成功删除%s为%s的数据", args[2], args[3])
	},
	PreRun: Connect,
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
