package analyzer

import (
	"bytes"
	"errors"
	"github.com/olivere/elastic"
	//"github.com/waitingkuo/elastic"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"text/template"
)

var esClient *elastic.Client
var mgoSession *mgo.Session

//var mongoHost = "localhost:3001"
var (
	termCriteria   map[string]string
	criteriaTerms  map[string][]string
	criteriaTraits map[string][]string
	traitTerms     map[string][]string
	allTerms       []string
	termWeights    map[string]float64
)

func init() {

	elasticsearchURL := os.Getenv("ELASTICSEARCH_URL")
	mongoURL := os.Getenv("MONGO_URL")

	if elasticsearchURL == "" {
		panic(errors.New("No ELASTICSEARCH_URL"))
	}
	if mongoURL == "" {
		panic(errors.New("No MONGO_URL"))
	}

	var err error
	esClient, err = elastic.NewClient(
		elastic.SetURL(elasticsearchURL),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}

	mgoSession, err = mgo.Dial(mongoURL)
	if err != nil {
		panic(err)
	}

	termCriteria = GetAllTermCriteria()
	criteriaTerms = GetAllCriteraTerms()
	criteriaTraits = GetAllCriteraTraits()
	traitTerms = GetAllTraitTerms()
	allTerms = GetAllTerms()
	termWeights = GetAllTermWeights()
}

func GetAllTermCriteria() map[string]string {
	session := mgoSession.Copy()
	defer session.Close()

	ret := make(map[string]string)

	type Resp struct {
		Term     string `bson:"term"`
		Criteria string `bson:"criteria"`
	}
	resp := Resp{}
	iter := session.DB("brandkoop").C("terms").Find(bson.M{}).Iter()
	for iter.Next(&resp) {
		ret[resp.Term] = resp.Criteria
	}

	return ret
}

func GetAllTermWeights() map[string]float64 {
	session := mgoSession.Copy()
	defer session.Close()

	ret := make(map[string]float64)

	type Resp struct {
		Term   string  `bson:"term"`
		Weight float64 `bson:"weight"`
	}
	resp := Resp{}
	iter := session.DB("brandkoop").C("terms").Find(bson.M{}).Iter()
	for iter.Next(&resp) {
		ret[resp.Term] = resp.Weight
	}

	return ret
}

func GetAllTraitTerms() map[string][]string {
	session := mgoSession.Copy()
	defer session.Close()

	pipe := session.DB("brandkoop").C("terms").Pipe([]bson.M{{
		"$group": bson.M{
			"_id":   "$trait",
			"terms": bson.M{"$addToSet": "$term"},
		},
	}})

	ret := make(map[string][]string)

	type Resp struct {
		Trait string   `bson:"_id"`
		Terms []string `bson:"terms"`
	}
	resp := Resp{}
	iter := pipe.Iter()
	for iter.Next(&resp) {
		ret[resp.Trait] = resp.Terms
	}

	return ret

}
func GetAllCriteraTraits() map[string][]string {
	session := mgoSession.Copy()
	defer session.Close()

	pipe := session.DB("brandkoop").C("terms").Pipe([]bson.M{{
		"$group": bson.M{
			"_id":    "$criteria",
			"traits": bson.M{"$addToSet": "$trait"},
		},
	}})

	ret := make(map[string][]string)

	type Resp struct {
		Criteria string   `bson:"_id"`
		Traits   []string `bson:"traits"`
	}
	resp := Resp{}
	iter := pipe.Iter()
	for iter.Next(&resp) {
		ret[resp.Criteria] = resp.Traits
	}

	return ret

}

func GetAllTerms() []string {
	session := mgoSession.Copy()
	defer session.Close()

	var result []string
	session.DB("brandkoop").C("terms").Find(bson.M{}).Distinct("term", &result)
	return result
}

func GetAllCriteraTerms() map[string][]string {
	// FIXME modify this when fix the auto disconnect isue
	session := mgoSession.Copy()
	defer session.Close()

	pipe := session.DB("brandkoop").C("terms").Pipe([]bson.M{{
		"$group": bson.M{
			"_id":   "$criteria",
			"terms": bson.M{"$addToSet": "$term"},
		},
	}})

	ret := make(map[string][]string)

	type Resp struct {
		Criteria string   `bson:"_id"`
		Terms    []string `bson:"terms"`
	}
	m := Resp{}
	iter := pipe.Iter()
	for iter.Next(&m) {
		ret[m.Criteria] = m.Terms
	}

	return ret

}

func MakeScript(terms []string) string {

	tmpl, err := template.New("script").Parse(
		`    sum=0; 
    for (t in [{{range $index, $element := .}}{{if ne $index 0}},{{end}}/{{$element.Term}}/:{{$element.Weight}}{{end}}])
      sum += _index['content'][t.key].tf() * t.value
    sum;
    `)
	/*
				`    sum=0;
		    for (t in [{{range $index, $element := .}}{{if ne $index 0}},{{end}}/{{$element}}/{{end}}])
		      sum += _index['content'][t].tf()
		    sum;
		    `)
	*/
	if err != nil {
		panic(err)
	}

	var doc bytes.Buffer
	type TermAndWeight struct {
		Term   string
		Weight float64
	}
	tw := []TermAndWeight{}
	for _, t := range terms {
		tw = append(tw, TermAndWeight{t, termWeights[t]})
	}
	tmpl.Execute(&doc, tw)

	//println(doc.String())

	return doc.String()
}

func TermFrequencyQuery(rootDomain string, termsMap map[string][]string) (*elastic.SearchResult, error) {

	termQuery := elastic.NewTermQuery("rootDomain", rootDomain)

	searchService := esClient.Search().
		Index("profiler").
		Type("page").
		Query(&termQuery)

	for field, terms := range termsMap {
		sumAggr := elastic.NewSumAggregation().Script(MakeScript(terms))
		searchService = searchService.Aggregation(field, sumAggr)
	}

	return searchService.Pretty(true).Do()

}

func ComputeCharacter(domainId string, rootDomain string) {
	result, err := TermFrequencyQuery(rootDomain, criteriaTerms)
	if err != nil {
		log.Printf("[Analyzer] [ERR] failed to compute character for domain: %s", err)
		return
	}

	mgoUpdate := bson.M{"$set": bson.M{}}
	for criteria, _ := range criteriaTerms {
		agg, _ := result.Aggregations.Sum(criteria)
		//println(criteria, *agg.Value)
		mgoUpdate["$set"].(bson.M)[criteria] = int(*agg.Value)
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("characters").Update(bson.M{"domainId": domainId}, mgoUpdate)
}

func ComputeValues(domainId string, rootDomain string) {
	result, err := TermFrequencyQuery(rootDomain, traitTerms)
	if err != nil {
		log.Printf("[Analyzer] [ERR] failed to compute values for domain: %s", err)
		return
	}

	//traitScores := make(map[string]
	type TraitScore struct {
		Criteria  string `bson:"criteria"`
		Trait     string `bson:"trait"`
		Frequency int    `bson:"frequency"`
	}
	traitScores := []TraitScore{}
	for criteria, traits := range criteriaTraits {
		for _, trait := range traits {
			agg, _ := result.Aggregations.Sum(trait)
			freq := int(*agg.Value)
			if freq == 0 {
				continue
			}
			traitScores = append(traitScores, TraitScore{
				Criteria:  criteria,
				Trait:     trait,
				Frequency: freq,
			})
			//log.Println(traitScores)

		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{
			"traits": traitScores,
		},
	}

	/*
		for trait, _ := range traitTerms {
			agg, _ := result.Aggregations.Sum(trait)
			//println(trait, *agg.Value)
			mgoUpdate["$set"].(bson.M)[trait] = int(*agg.Value)
		}
	*/

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("values").Update(bson.M{"domainId": domainId}, mgoUpdate)
}

func ComputeWordCloud(domainId string, rootDomain string) {
	//FIXME a hack
	myTerms := make(map[string][]string)
	for _, t := range allTerms {
		myTerms[t] = []string{t}
	}

	result, err := TermFrequencyQuery(rootDomain, myTerms)
	if err != nil {
		log.Printf("[Analyzer] [ERR] failed to compute word cloud for domain: %s", err)
		return
	}

	/*
		mgoUpdate := bson.M{"$set": bson.M{"words": []bson.M{}}}
		for term, _ := range myTerms {
			agg, _ := result.Aggregations.Sum(term)
			//println(term, *agg.Value)
			//mgoUpdate["$set"].(bson.M)[term] = int(*agg.Value)
			count := int(*agg.Value)
			if count == 0 {
				continue
			}
			mgoUpdate["$set"].(bson.M)["words"] = append(mgoUpdate["$set"].(bson.M)["words"].([]bson.M), bson.M{"word": term, "size": count})
		}
	*/
	type Word struct {
		Text     string `bson:"text"`
		Size     int    `bson:"size"`
		Criteria string `bson:"criteria"`
	}
	words := []Word{}
	for term, _ := range myTerms {
		agg, _ := result.Aggregations.Sum(term)
		count := int(*agg.Value)
		if count == 0 {
			continue
		}
		words = append(words, Word{term, count, termCriteria[term]})
	}
	mgoUpdate := bson.M{
		"$set": bson.M{"words": words},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("wordclouds").Update(bson.M{"domainId": domainId}, mgoUpdate)
}
