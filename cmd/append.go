package cmd

import (
	"esync/dblayer"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var appendCmd = &cobra.Command{
	Use:   "append Index Type",
	Short: "导入数据",
	Long:  `根据相应文件,导入新增数据`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.Append(&dblayer.LoadRequest{
			Dir:   dir,
			Index: args[0],
			Type:  args[1],
		})
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Println("恢复成功")
	},
	PreRun: Connect,
}

func init() {
	rootCmd.AddCommand(appendCmd)
	appendCmd.Flags().StringVarP(&dir, "dir", "d", "backup",
		"加载文件保存路径")
}
