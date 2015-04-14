package es

import (
	"errors"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/waitingkuo/brandkoop-profiler/util"
	"github.com/waitingkuo/domainutil"
	"time"
	//"github.com/waitingkuo/elastic"
	"os"
)

var esClient *elastic.Client

func init() {

	elasticsearchURL := os.Getenv("ELASTICSEARCH_URL")

	if elasticsearchURL == "" {
		panic(errors.New("No ELASTICSEARCH_URL"))
	}

	var err error
	esClient, err = elastic.NewClient(
		elastic.SetURL(elasticsearchURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheckTimeout(time.Second*10),
	)
	if err != nil {
		fmt.Println("cannot connect to" + elasticsearchURL)
		panic(err)
	}
}

type Page struct {
	Domain     string `json:"domain"`
	RootDomain string `json:"rootDomain"`
	SubDomain  string `json":subDomain"`
	Url        string `json:"url"`
	Content    string `json:"content"`
}

func IndexPage(domain *domainutil.Domain, url string, content string) error {

	id := util.Hash(url)

	page := Page{
		Domain:     domain.String(),
		RootDomain: domain.RootDomain,
		SubDomain:  domain.SubDomain,
		Url:        url,
		Content:    content,
	}

	_, err := esClient.Index().
		Index("profiler").
		Type("page").
		Id(id).
		BodyJson(page).
		Do()
	if err != nil {
		return err
	}

	return nil
}
