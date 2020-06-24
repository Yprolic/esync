package cmd

import (
	"esync/dblayer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var backupCmd = &cobra.Command{
	Use:   "backup Index Type",
	Short: "备份索引",
	Long:  `将Es指定索引全量Dump到指定路径下`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		err := esSearch.Backup(&dblayer.BackupRequest{
			Dir:   dir,
			Index: args[0],
			Type:  args[1],
		})
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Println("备份成功")
	},
	PreRun: Connect,
}

func init() {
	rootCmd.AddCommand(backupCmd)
	backupCmd.Flags().StringVarP(&dir, "dir", "d", "backup",
		"备份文件保存路径")
}
