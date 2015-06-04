package es

import (
	"errors"
	"fmt"
	//"encoding/json"
	"github.com/olivere/elastic"
	"github.com/waitingkuo/brandkoop-profiler/util"
	"github.com/waitingkuo/domainutil"
	"time"
	//"github.com/waitingkuo/elastic"
	"encoding/json"
	"net/url"
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
	PageWeight int    `json:"pageWeight"`
}

//func GetTermVector(domain *domainutil.Domain, url string) {
func GetIdsByRootDomain(rootDomain string) []string {

	query := elastic.NewTermQuery("rootDomain", rootDomain)
	result, err := esClient.Search().
		Index("profiler").
		Type("page").
		Query(query).
		Size(1000).
		Fields().
		Do()

	if err != nil {
		fmt.Println("failed to get ids by domain")
	}

	ids := []string{}
	for _, hit := range result.Hits.Hits {
		ids = append(ids, hit.Id)
	}

	return ids
}

func GetTermVectorById(id string) map[string]int {

	params := url.Values{}
	body := map[string]bool{
		"offsets":   false,
		"payloads":  false,
		"positions": false,
	}

	res, err := esClient.PerformRequest("GET", "/profiler/page/"+id+"/_termvector?fields=content", params, body)
	if err != nil {
		fmt.Println("failed to get term frequecy of")
	}

	// not readable, should find a better way to deal with json
	t1 := make(map[string]*json.RawMessage)
	t2 := make(map[string]*json.RawMessage)
	t3 := make(map[string]*json.RawMessage)
	t4 := make(map[string]map[string]int)

	ret := make(map[string]int)

	err = json.Unmarshal(res.Body, &t1)
	if err != nil {
		fmt.Println("error when unmarshal t1")
	}

	if t1["term_vectors"] == nil {
		return ret
	}
	err = json.Unmarshal(*t1["term_vectors"], &t2)
	if err != nil {
		fmt.Println("error when unmarshal t2")
	}

	if t2["content"] == nil {
		return ret
	}
	err = json.Unmarshal(*t2["content"], &t3)
	if err != nil {
		fmt.Println("error when unmarshal t3")
	}

	if t3["terms"] == nil {
		return ret
	}
	err = json.Unmarshal(*t3["terms"], &t4)
	if err != nil {
		fmt.Println("error when unmarshal t4")
	}

	if t4 == nil {
		return ret
	}
	for term, termFreqMap := range t4 {
		ret[term] = termFreqMap["term_freq"]
	}

	return ret
}

func IndexPage(domain *domainutil.Domain, url string, content string) error {

	id := util.Hash(url)

	page := Page{
		Domain:     domain.String(),
		RootDomain: domain.RootDomain,
		SubDomain:  domain.SubDomain,
		Url:        url,
		Content:    content,
		PageWeight: 1,
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
