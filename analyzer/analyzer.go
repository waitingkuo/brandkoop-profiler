package analyzer

import (
	"bytes"
	"errors"
	"github.com/ChimeraCoder/anaconda"
	"github.com/olivere/elastic"
	"net/url"
	"strconv"
	"time"
	//"github.com/waitingkuo/elastic"
	"fmt"
	"github.com/waitingkuo/brandkoop-profiler/es"
	"github.com/waitingkuo/brandkoop-profiler/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"regexp"
	"text/template"
)

var esClient *elastic.Client
var mgoSession *mgo.Session

//var mongoHost = "localhost:3001"
var (
	termCriteria             map[string]string
	criteriaTerms            map[string][]string
	criteriaTraits           map[string][]string
	traitTerms               map[string][]string
	allTerms                 []string
	termWeights              map[string]float64
	twitterConsumerKey       string
	twitterConsumerSecret    string
	twitterAccessToken       string
	twitterAccessTokenSecret string
)

func init() {

	elasticsearchURL := os.Getenv("ELASTICSEARCH_URL")
	mongoURL := os.Getenv("MONGO_URL")

	twitterConsumerKey = os.Getenv("TWITTER_CONSUMER_KEY")
	twitterConsumerSecret = os.Getenv("TWITTER_CONSUMER_SECRET")
	twitterAccessToken = os.Getenv("TWITTER_ACESS_TOKEN")
	twitterAccessTokenSecret = os.Getenv("TWITTER_ACESS_TOKEN_SECRET")

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
		elastic.SetHealthcheckTimeout(time.Second*10),
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

func MakeScript(terms []string, weightedUrls []string) string {

	weightedDocIds := []string{}
	for _, url := range weightedUrls {
		weightedDocIds = append(weightedDocIds, util.Hash(url))
	}
	tmpl, err := template.New("script").Parse(
		`    sum=0; 
    docId = doc["_uid"].value.split("#")[1]
    for (t in [{{range $index, $element := .TermWeight}}{{if ne $index 0}},{{end}}/{{$element.Term}}/:{{$element.Weight}}{{end}}])
      if ([{{range $index2, $docId := .WeightedDocIds}}{{if ne $index2 0}},{{end}}"{{$docId}}"{{end}}].contains(docId)) {
        sum += _index['content'][t.key].tf() * t.value * 10
      }
      else {
        sum += _index['content'][t.key].tf() * t.value
      }
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
	type Params struct {
		TermWeight     []TermAndWeight
		WeightedDocIds []string
	}
	params := Params{tw, weightedDocIds}
	tmpl.Execute(&doc, params)

	//println(doc.String())

	return doc.String()
}

func TermFrequencyQuery(rootDomain string, termsMap map[string][]string, weightedUrls []string) (*elastic.SearchResult, error) {

	termQuery := elastic.NewTermQuery("rootDomain", rootDomain)

	searchService := esClient.Search().
		Index("profiler").
		Type("page").
		Query(&termQuery)

	for field, terms := range termsMap {
		sumAggr := elastic.NewSumAggregation().Script(MakeScript(terms, weightedUrls))
		searchService = searchService.Aggregation(field, sumAggr)
	}

	return searchService.Pretty(true).Do()

}

func ComputeCharacter(domainId string, rootDomain string, weightedUrls []string) {
	result, err := TermFrequencyQuery(rootDomain, criteriaTerms, weightedUrls)
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

func ComputeTweetCharacter(screenName string) (map[string]int, map[string]int, error) {
	anaconda.SetConsumerKey(twitterConsumerKey)
	anaconda.SetConsumerSecret(twitterConsumerSecret)
	api := anaconda.NewTwitterApi(twitterAccessToken, twitterAccessTokenSecret)

	wordFrequency := make(map[string]int)

	isFirst := true
	var lastMinId int64 = -1
	for true {
		values := make(url.Values)
		values.Add("screen_name", screenName)
		values.Add("count", "200")
		if !isFirst {
			values.Add("max_id", strconv.FormatInt(lastMinId, 10))
		}

		timeline, err := api.GetUserTimeline(values)
		if err != nil {
			return nil, nil, err
		}

		if isFirst {
			if len(timeline) == 0 {
				break
			}
		} else {
			if len(timeline) == 0 || len(timeline) == 1 {
				break
			}
			timeline = timeline[1:]
		}

		for _, tweet := range timeline {
			words := regexp.MustCompile(" +").Split(tweet.Text, -1)
			for _, word := range words {
				if termWeights[word] > 0 {
					wordFrequency[word] += 1
				}
			}
		}

		lastMinId = timeline[len(timeline)-1].Id

		isFirst = false
	}

	characterScores := make(map[string]int)
	for criteria, terms := range criteriaTerms {
		freq := 0.0
		for _, term := range terms {
			freq += termWeights[term] * float64(wordFrequency[term])
		}
		characterScores[criteria] = int(freq)
	}

	return characterScores, wordFrequency, nil
}

func ComputeValues(domainId string, rootDomain string, weightedUrls []string) {
	result, err := TermFrequencyQuery(rootDomain, traitTerms, weightedUrls)
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

func ComputeWordCloud(domainId string, rootDomain string, weightedUrls []string) {
	//FIXME a hack
	myTerms := make(map[string][]string)
	for _, t := range allTerms {
		myTerms[t] = []string{t}
	}

	result, err := TermFrequencyQuery(rootDomain, myTerms, weightedUrls)
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

/*
 * V2
 */
func GetTwitterTermFrequency(screenName string) map[string]float64 {

	anaconda.SetConsumerKey(twitterConsumerKey)
	anaconda.SetConsumerSecret(twitterConsumerSecret)
	api := anaconda.NewTwitterApi(twitterAccessToken, twitterAccessTokenSecret)
	termFreq := make(map[string]float64)

	isFirst := true
	var lastMinId int64 = -1
	for true {
		values := make(url.Values)
		values.Add("screen_name", screenName)
		values.Add("count", "200")
		if !isFirst {
			values.Add("max_id", strconv.FormatInt(lastMinId, 10))
		}

		timeline, err := api.GetUserTimeline(values)
		if err != nil {
			//FIXME
			return nil
		}

		if isFirst {
			if len(timeline) == 0 {
				break
			}
		} else {
			if len(timeline) == 0 || len(timeline) == 1 {
				break
			}
			timeline = timeline[1:]
		}

		for _, tweet := range timeline {
			words := regexp.MustCompile(" +").Split(tweet.Text, -1)
			for _, word := range words {
				termWeight, ok := termWeights[word]
				if ok {
					termFreq[word] += termWeight
				}
			}
		}

		lastMinId = timeline[len(timeline)-1].Id

		isFirst = false
	}

	return termFreq

}
func GetDomainTermFrequency(rootDomain string, weightedUrls []string) map[string]float64 {
	//termWeight
	termFreq := make(map[string]float64)

	weightedDocIds := make(map[string]int)
	for _, u := range weightedUrls {
		weightedDocIds[util.Hash(u)] = 10
	}

	//fmt.Println(termWeights)

	docIds := es.GetIdsByRootDomain(rootDomain)
	for _, docId := range docIds {
		vector := es.GetTermVectorById(docId)
		docWeight := 1
		weightedDocWeight, ok := weightedDocIds[docId]
		if ok {
			docWeight = weightedDocWeight
		}
		for term, freq := range vector {
			termWeight, ok := termWeights[term]
			if ok {
				termFreq[term] += float64(freq) * float64(docWeight) * termWeight
			}
		}
	}

	return termFreq
}

func ComputeCharacterV2(domainId string, termFreq map[string]float64) {

	mgoUpdate := bson.M{"$set": bson.M{}}
	for criteria, terms := range criteriaTerms {
		freq := 0.0
		for _, term := range terms {
			f, ok := termFreq[term]
			if ok {
				freq += f
			}
		}
		mgoUpdate["$set"].(bson.M)[criteria] = freq
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("characters").Update(bson.M{"domainId": domainId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}
func ComputeTwitterCharacterV2(twitterId string, termFreq map[string]float64) {

	mgoUpdate := bson.M{"$set": bson.M{}}
	for criteria, terms := range criteriaTerms {
		freq := 0.0
		for _, term := range terms {
			f, ok := termFreq[term]
			if ok {
				freq += f
			}
		}
		mgoUpdate["$set"].(bson.M)[criteria] = freq
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterCharacters").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}

func ComputeValuesV2(domainId string, termFreq map[string]float64) {

	type TraitScore struct {
		Criteria  string `bson:"criteria"`
		Trait     string `bson:"trait"`
		Frequency int    `bson:"frequency"`
	}
	traitScores := []TraitScore{}
	for criteria, traits := range criteriaTraits {
		for _, trait := range traits {
			freq := 0.0
			for _, term := range traitTerms[trait] {
				f, ok := termFreq[term]
				if ok {
					freq += f
				}
			}
			if int(freq) > 0 {
				traitScores = append(traitScores, TraitScore{
					Criteria:  criteria,
					Trait:     trait,
					Frequency: int(freq),
				})
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{
			"traits": traitScores,
		},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("values").Update(bson.M{"domainId": domainId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}
func ComputeTwitterValuesV2(twitterId string, termFreq map[string]float64) {

	type TraitScore struct {
		Criteria  string `bson:"criteria"`
		Trait     string `bson:"trait"`
		Frequency int    `bson:"frequency"`
	}
	traitScores := []TraitScore{}
	for criteria, traits := range criteriaTraits {
		for _, trait := range traits {
			freq := 0.0
			for _, term := range traitTerms[trait] {
				f, ok := termFreq[term]
				if ok {
					freq += f
				}
			}
			if int(freq) > 0 {
				traitScores = append(traitScores, TraitScore{
					Criteria:  criteria,
					Trait:     trait,
					Frequency: int(freq),
				})
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{
			"traits": traitScores,
		},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterValues").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}

type Word struct {
	Term     string `bson:"term"`
	Freq     int    `bson:"freq"`
	Criteria string `bson:"criteria"`
}

func ComputeWordcloudV2(domainId string, termFreq map[string]float64) {

	words := []Word{}
	for criteria, terms := range criteriaTerms {
		for _, term := range terms {
			freq, ok := termFreq[term]
			if ok {
				if int(freq) > 0 {
					words = append(words, Word{term, int(freq), criteria})
				}
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{"words": words},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("wordclouds").Update(bson.M{"domainId": domainId}, mgoUpdate)
	//fmt.Println(mgoUpdate)

}
func ComputeTwitterWordcloudV2(twitterId string, termFreq map[string]float64) {

	words := []Word{}
	for criteria, terms := range criteriaTerms {
		for _, term := range terms {
			freq, ok := termFreq[term]
			if ok {
				if int(freq) > 0 {
					words = append(words, Word{term, int(freq), criteria})
				}
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{"words": words},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterWordclouds").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	//fmt.Println(mgoUpdate)

}

/*
 * v3
 */

func ComputeWebsiteCharacterV3(websiteId string, termFreq map[string]float64) {

	mgoUpdate := bson.M{"$set": bson.M{}}
	for criteria, terms := range criteriaTerms {
		freq := 0.0
		for _, term := range terms {
			f, ok := termFreq[term]
			if ok {
				freq += f
			}
		}
		mgoUpdate["$set"].(bson.M)[criteria] = freq
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("websiteCharacters").Update(bson.M{"websiteId": websiteId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}
func ComputeTwitterCharacterV3(twitterId string, termFreq map[string]float64) {

	mgoUpdate := bson.M{"$set": bson.M{}}
	for criteria, terms := range criteriaTerms {
		freq := 0.0
		for _, term := range terms {
			f, ok := termFreq[term]
			if ok {
				freq += f
			}
		}
		mgoUpdate["$set"].(bson.M)[criteria] = freq
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterCharacters").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}

func ComputeWebsiteValuesV3(websiteId string, termFreq map[string]float64) {

	type TraitScore struct {
		Criteria  string `bson:"criteria"`
		Trait     string `bson:"trait"`
		Frequency int    `bson:"frequency"`
	}
	traitScores := []TraitScore{}
	for criteria, traits := range criteriaTraits {
		for _, trait := range traits {
			freq := 0.0
			for _, term := range traitTerms[trait] {
				f, ok := termFreq[term]
				if ok {
					freq += f
				}
			}
			if int(freq) > 0 {
				traitScores = append(traitScores, TraitScore{
					Criteria:  criteria,
					Trait:     trait,
					Frequency: int(freq),
				})
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{
			"traits": traitScores,
		},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("websiteValues").Update(bson.M{"websiteId": websiteId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}
func ComputeTwitterValuesV3(twitterId string, termFreq map[string]float64) {

	type TraitScore struct {
		Criteria  string `bson:"criteria"`
		Trait     string `bson:"trait"`
		Frequency int    `bson:"frequency"`
	}
	traitScores := []TraitScore{}
	for criteria, traits := range criteriaTraits {
		for _, trait := range traits {
			freq := 0.0
			for _, term := range traitTerms[trait] {
				f, ok := termFreq[term]
				if ok {
					freq += f
				}
			}
			if int(freq) > 0 {
				traitScores = append(traitScores, TraitScore{
					Criteria:  criteria,
					Trait:     trait,
					Frequency: int(freq),
				})
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{
			"traits": traitScores,
		},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterValues").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	fmt.Println(mgoUpdate)

}

func ComputeWebsiteWordcloudV3(websiteId string, termFreq map[string]float64) {

	words := []Word{}
	for criteria, terms := range criteriaTerms {
		for _, term := range terms {
			freq, ok := termFreq[term]
			if ok {
				if int(freq) > 0 {
					words = append(words, Word{term, int(freq), criteria})
				}
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{"words": words},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("websiteWordclouds").Update(bson.M{"websiteId": websiteId}, mgoUpdate)
	//fmt.Println(mgoUpdate)

}
func ComputeTwitterWordcloudV3(twitterId string, termFreq map[string]float64) {

	words := []Word{}
	for criteria, terms := range criteriaTerms {
		for _, term := range terms {
			freq, ok := termFreq[term]
			if ok {
				if int(freq) > 0 {
					words = append(words, Word{term, int(freq), criteria})
				}
			}
		}
	}
	mgoUpdate := bson.M{
		"$set": bson.M{"words": words},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("twitterWordclouds").Update(bson.M{"twitterId": twitterId}, mgoUpdate)
	//fmt.Println(mgoUpdate)

}

func SetWebsiteProfiled(websiteId string) {
	mgoUpdate := bson.M{
		"$set": bson.M{"profiled": true},
	}

	session := mgoSession.Copy()
	defer session.Close()
	session.DB("meteor").C("websites").Update(bson.M{"_id": websiteId}, mgoUpdate)
	//fmt.Println(mgoUpdate)
}
