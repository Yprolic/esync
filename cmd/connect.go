package cmd

import (
	"esync/dblayer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
)

type EsConf struct {
	Url      string
	UserName string
	Password string
	Index    string
}
type Conf struct {
	Es EsConf
}

var (
	conf     Conf
	esSearch dblayer.ElasticSearch
)

func Connect(cmd *cobra.Command, args []string) {
	if err := esSearch.Connect(&dblayer.Connection{
		User:     conf.Es.UserName,
		Password: conf.Es.Password,
		Address:  conf.Es.Url,
	}); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}
