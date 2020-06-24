package dblayer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/olivere/elastic"

	"github.com/sirupsen/logrus"
)

type ElasticSearch struct {
	client *elastic.Client
}

const (
	bulkSize = 1000

	delimiter       = '\n'
	mappingFileName = "mapping.json"
	recordFileName  = "records.json"
)

func (es *ElasticSearch) Connect(c *Connection) error {
	client, err := elastic.NewClient(
		elastic.SetURL(fmt.Sprintf("http://%s", c.Address)),
		elastic.SetBasicAuth(c.User, c.Password),
	)
	if err != nil {
		return err
	}

	// todo 检测连接是否成功
	es.client = client
	return nil
}

func (es *ElasticSearch) Close() error {
	es.client.Stop()
	return nil
}

func (es *ElasticSearch) SetOrUpdate(request *SetOrUpdateRequest) error {
	count := 0
	bulk := es.client.Bulk()
	for _, d := range request.Data {
		if d.ID == "" {
			bulk.Add(
				elastic.NewBulkIndexRequest().
					Index(request.Index).
					Doc(d.Data).
					Type(request.Type),
			)
		} else {
			bulk.Add(
				elastic.NewBulkIndexRequest().
					Index(request.Index).
					Doc(d.Data).
					Id(d.ID).
					Type(request.Type),
			)
		}
		count++
		if count >= bulkSize {
			if err := bulkDo(bulk); err != nil {
				return err
			}
			count = 0
		}
	}

	if err := bulkDo(bulk); err != nil {
		return err
	}

	return nil
}

func (es *ElasticSearch) Delete(request *DeleteRequest) error {
	_, err := es.client.DeleteByQuery().Index(request.Index).Type(request.Type).
		Query(elastic.NewTermsQuery(request.Key, request.Value)).
		Conflicts("proceed").Do(context.TODO())
	if err != nil {
		return fmt.Errorf("删除报错:%s", err.Error())
	}
	return nil
}

func (es *ElasticSearch) GetMapping(Index, Type string) (interface{}, error) {
	resp, err := es.client.GetMapping().Index(Index).Do(context.TODO())
	if err != nil {
		return nil, err
	}

	if schemaSettings, ok := resp[Index]; !ok {
		return nil, fmt.Errorf("mapping 不存在: %s", Index)
	} else {
		b, err := json.Marshal(schemaSettings)
		if err != nil {
			return nil, fmt.Errorf("ES mapping 格式非法: %s", Index)
		}
		var m map[string]map[string]interface{}
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		} else {
			if t, ok := m["mappings"]; ok {
				if i, ok := t[Type]; ok {
					return i, nil
				} else {
					return nil, fmt.Errorf("mapping 中没有 %s 的设置", Type)
				}
			} else {
				return nil, fmt.Errorf("未能获取 mapping ")
			}
		}
	}
}
func saveToFile(dir string, data interface{}) error {
	file, err := os.Create(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	if content, err := json.Marshal(data); err == nil {
		if _, err := file.Write(content); err != nil {
			return fmt.Errorf("文件写入失败: %s", err.Error())
		}
	} else {
		return fmt.Errorf("数据格式异常: %s", err.Error())
	}
	return nil
}

func (es *ElasticSearch) Backup(request *BackupRequest) error {
	if _, err := os.Stat(request.Dir); os.IsNotExist(err) {
		if err := os.Mkdir(request.Dir, 0777); err != nil {
			return fmt.Errorf("创建文件夹失败: %s", err.Error())
		}
	}

	recordFile, err := os.Create(fmt.Sprintf("%s/%s", request.Dir, recordFileName))
	if err != nil {
		return err
	}
	defer recordFile.Close()

	mapping, err := es.GetMapping(request.Index, request.Type)
	if err != nil {
		return err
	}

	err = saveToFile(fmt.Sprintf("%s/%s", request.Dir, mappingFileName), mapping)
	if err != nil {
		return err
	}

	// 数据保存
	w := bufio.NewWriter(recordFile)
	scroll := es.client.Scroll(request.Index).Size(bulkSize)
	for {
		if results, err := scroll.Do(context.Background()); err != nil {
			if err == io.EOF {
				break
			} else {
				logrus.Error(err.Error())
				return err
			}
		} else {
			println("结果数量", len(results.Hits.Hits))
			for _, r := range results.Hits.Hits {
				if t, err := r.Source.MarshalJSON(); err != nil {
					return err
				} else {
					w.WriteString(fmt.Sprintf("%s%c", string(t), delimiter))
				}
			}
		}
	}
	return w.Flush()
}

func (es *ElasticSearch) Drop(index string) error {
	_, err := es.client.DeleteIndex(index).Do(context.TODO())
	if err != nil {
		return err
	}
	return nil
}

func (es *ElasticSearch) Load(request *LoadRequest) error {
	mappingContent, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", request.Dir, mappingFileName))
	if err != nil {
		return fmt.Errorf("打开 mapping 文件失败: %s", err.Error())
	}

	recordFile, err := os.Open(fmt.Sprintf("%s/%s", request.Dir, recordFileName))
	if err != nil {
		return fmt.Errorf("打开记录文件失败: %s", err.Error())
	}
	defer recordFile.Close()

	if ok, err := es.client.IndexExists(request.Index).Do(context.TODO()); err != nil {
		return fmt.Errorf("索引检查失败: %s", err.Error())
	} else {
		if ok {
			return fmt.Errorf("已存在索引: %s, 请先删除该索引", request.Index)
		}
	}

	if _, err := es.client.CreateIndex(request.Index).Do(context.TODO()); err != nil {
		return fmt.Errorf("创建索引失败: %s", err.Error())
	}
	if _, err := es.client.PutMapping().Index(request.Index).Type(request.Type).BodyString(string(mappingContent)).Do(context.TODO()); err != nil {
		return fmt.Errorf("上传 mapping 失败: %s", err.Error())
	}

	r := bufio.NewReader(recordFile)
	bulk := es.client.Bulk()
	count := 0
	for {
		if content, err := r.ReadString(delimiter); err != nil {
			if err == io.EOF {
				break
			} else {
				return fmt.Errorf("读取数据记录异常: %s", err.Error())
			}
		} else {
			bulk.Add(
				elastic.NewBulkIndexRequest().
					Index(request.Index).
					Doc(content).
					Type(request.Type),
			)
			count++
			if count >= bulkSize {
				if err := bulkDo(bulk); err != nil {
					return fmt.Errorf("bulk 操作失败: %s", err.Error())
				}
				count = 0
			}
		}
	}
	return bulkDo(bulk)
}

func (es *ElasticSearch) Append(request *LoadRequest) error {

	recordFile, err := os.Open(fmt.Sprintf("%s/%s", request.Dir, recordFileName))
	if err != nil {
		return fmt.Errorf("打开记录文件失败: %s", err.Error())
	}
	defer recordFile.Close()

	r := bufio.NewReader(recordFile)
	bulk := es.client.Bulk()
	count := 0
	for {
		if content, err := r.ReadString(delimiter); err != nil {
			if err == io.EOF {
				break
			} else {
				return fmt.Errorf("读取数据记录异常: %s", err.Error())
			}
		} else {
			bulk.Add(
				elastic.NewBulkIndexRequest().
					Index(request.Index).
					Doc(content).
					Type(request.Type),
			)
			count++
			if count >= bulkSize {
				if err := bulkDo(bulk); err != nil {
					return fmt.Errorf("bulk 操作失败: %s", err.Error())
				}
				count = 0
			}
		}
	}
	return bulkDo(bulk)
}

func (es *ElasticSearch) ExistSchema(request *ExistSchemaRequest) (bool, error) {
	return es.client.IndexExists(request.Schema).Do(context.TODO())
}

func (es *ElasticSearch) SetAlias(alias string, index string) error {
	aliases, err := es.client.CatAliases().Alias(alias).Pretty(true).Do(context.TODO())
	if err != nil {
		return err
	}
	for _, aliasItem := range aliases {
		_, err := es.client.Alias().Remove(aliasItem.Index, aliasItem.Alias).Do(context.TODO())
		if err != nil {
			return err
		}
	}
	_, err = es.client.Alias().Action(elastic.NewAliasAddAction(alias).Index(index)).Do(context.TODO())
	return err
}

func bulkDo(bulk *elastic.BulkService) error {
	if bulk.NumberOfActions() == 0 {
		return nil
	}

	myCtx, _ := context.WithTimeout(context.TODO(), time.Duration(3)*time.Second)
	resp, err := bulk.Do(myCtx)
	t, _ := json.Marshal(resp)
	logrus.Infof("bulkDo response : %s", string(t))
	if err != nil {
		return err
	} else if resp.Errors {
		failedRequest := resp.Failed()[0]
		return fmt.Errorf("%s:%s", failedRequest.Error.Type, failedRequest.Error.Reason)
	}

	return nil
}
