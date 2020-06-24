package cmd

import (
	"esync/dblayer"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var loadCmd = &cobra.Command{
	Use:   "load Index Type",
	Short: "导入数据",
	Long:  `根据相应文件,新建索引并导入全部数据`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.Load(&dblayer.LoadRequest{
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
	rootCmd.AddCommand(loadCmd)
	loadCmd.Flags().StringVarP(&dir, "dir", "d", "backup",
		"加载文件保存路径")
}
