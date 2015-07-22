package main

import (
	"github.com/gin-gonic/gin"
	//"net/url"
	"fmt"
	"github.com/waitingkuo/brandkoop-profiler/analyzer"
	"github.com/waitingkuo/brandkoop-profiler/crawler"
	//"github.com/waitingkuo/brandkoop-profiler/es"
	"github.com/waitingkuo/domainutil"
	"log"
	"net/http"
	"strings"
)

func main() {

	router := gin.Default()

	/*
	 * V3
	 */
	router.POST("/v3/analyzer/analyzewebsite", func(c *gin.Context) {
		c.Request.ParseForm()

		rawDomain := c.Request.Form.Get("domain")
		websiteId := c.Request.Form.Get("websiteId")
		domain, err := domainutil.ParseFromHost(rawDomain)
		if err != nil {
			fmt.Println("[ERROR] failed to parse domain ", rawDomain)
			return
		}
		//weightedUrls := c.Request.Form["weightedUrls"]
		go func() {
			termFreq := analyzer.GetDomainTermFrequency(domain.RootDomain, []string{})
			fmt.Println("Analyzing Website Character ...", domain.RootDomain)
			analyzer.ComputeWebsiteCharacterV3(websiteId, termFreq)
			fmt.Println("Analyzing Website Values ... ", domain.RootDomain)
			analyzer.ComputeWebsiteValuesV3(websiteId, termFreq)
			fmt.Println("Analyzing Website Cloud ...", domain.RootDomain)
			analyzer.ComputeWebsiteWordcloudV3(websiteId, termFreq)
			analyzer.SetWebsiteProfiled(websiteId)
			fmt.Println("Done ...")
		}()
	})
	router.POST("/v3/profiler/profilewebsite", func(c *gin.Context) {
		c.Request.ParseForm()

		rawDomain := c.Request.Form.Get("domain")
		websiteId := c.Request.Form.Get("websiteId")
		domain, err := domainutil.ParseFromHost(rawDomain)
		if err != nil {
			fmt.Println("[ERROR] failed to parse domain ", rawDomain)
			return
		}
		fmt.Println(domain)
		go func() {
			//crawl FIXME to move to another function
			seed := "http://" + rawDomain
			resp, err := http.Head("http://" + rawDomain)
			if err != nil {
				log.Printf("[Err] Head %s: %s", seed, err)
				return
			}
			seed = resp.Request.URL.String()

			domain, err := domainutil.ParseFromRawURL(seed)
			if err != nil {
				log.Printf("[Err] Failed to parse seed %s: %s", seed, err)
				return
			}

			crawler.FullCrawl(domain, seed, 100)

			fmt.Println("Get domain term frequceny ...", domain.RootDomain)
			termFreq := analyzer.GetDomainTermFrequency(domain.RootDomain, []string{})
			fmt.Println("Analyzing Website Character ...", domain.RootDomain)
			analyzer.ComputeWebsiteCharacterV3(websiteId, termFreq)
			fmt.Println("Analyzing Website Values ... ", domain.RootDomain)
			analyzer.ComputeWebsiteValuesV3(websiteId, termFreq)
			fmt.Println("Analyzing Website Cloud ...", domain.RootDomain)
			analyzer.ComputeWebsiteWordcloudV3(websiteId, termFreq)
			analyzer.SetWebsiteProfiled(websiteId)
			fmt.Println("Done ...")
		}()
	})
	/*
	 * V2 : for old dashboard, will be deprecated soon
	 */
	router.POST("/v2/cralwer/crawldomain", func(c *gin.Context) {
	})
	router.GET("/v2/test", func(c *gin.Context) {
		domain, _ := domainutil.ParseFromHost("pic-collage.com")
		seed := "http://pic-collage.com"
		crawler.FullCrawl(domain, seed, 100)
	})
	router.POST("/v2/crawler/crawldomain", func(c *gin.Context) {
		c.Request.ParseForm()
		go func() {
		}()
	})
	router.POST("/v2/analyzer/analyzedomain", func(c *gin.Context) {
		c.Request.ParseForm()

		rawDomain := c.Request.Form.Get("domain")
		domainId := c.Request.Form.Get("domainId")
		domain, err := domainutil.ParseFromHost(rawDomain)
		if err != nil {
			fmt.Println("[ERROR] failed to parse domain ", rawDomain)
			return
		}
		//weightedUrls := c.Request.Form["weightedUrls"]
		go func() {
			termFreq := analyzer.GetDomainTermFrequency(domain.RootDomain, []string{})
			fmt.Println("Analyzing Domain Character ...", domain.RootDomain)
			analyzer.ComputeCharacterV2(domainId, termFreq)
			fmt.Println("Analyzing Domain Values ...", domain.RootDomain)
			analyzer.ComputeValuesV2(domainId, termFreq)
			fmt.Println("Analyzing Domain Wordcloud ...", domain.RootDomain)
			analyzer.ComputeWordcloudV2(domainId, termFreq)
			fmt.Println("Done ...")
		}()
	})

	router.POST("/v2/profiler/profiletwitter", func(c *gin.Context) {
		c.Request.ParseForm()

		twitterId := c.Request.Form.Get("twitterId")
		screenName := c.Request.Form.Get("screenName")

		termFreq := analyzer.GetTwitterTermFrequency(screenName)
		fmt.Println("Analyzing Twitter Character ...")
		analyzer.ComputeTwitterCharacterV2(twitterId, termFreq)
		fmt.Println("Analyzing Twitter Values ...")
		analyzer.ComputeTwitterValuesV2(twitterId, termFreq)
		fmt.Println("Analyzing Twitter Wordcloud ...")
		analyzer.ComputeTwitterWordcloudV2(twitterId, termFreq)
		fmt.Println("Analyzing Done ...")

	})

	router.POST("/v2/profiler/profiledomain", func(c *gin.Context) {
		c.Request.ParseForm()

		rawDomain := c.Request.Form.Get("domain")
		domainId := c.Request.Form.Get("domainId")
		domain, err := domainutil.ParseFromHost(rawDomain)
		if err != nil {
			fmt.Println("[ERROR] failed to parse domain ", rawDomain)
			return
		}
		fmt.Println(domain)
		go func() {
			//crawl FIXME to move to another function
			seed := "http://" + rawDomain
			resp, err := http.Head("http://" + rawDomain)
			if err != nil {
				log.Printf("[Err] Head %s: %s", seed, err)
				return
			}
			seed = resp.Request.URL.String()

			domain, err := domainutil.ParseFromRawURL(seed)
			if err != nil {
				log.Printf("[Err] Failed to parse seed %s: %s", seed, err)
				return
			}

			crawler.FullCrawl(domain, seed, 100)

			fmt.Println("Get domain term frequceny ...", domain.RootDomain)
			termFreq := analyzer.GetDomainTermFrequency(domain.RootDomain, []string{})
			fmt.Println("Analyzing Website Character ...", domain.RootDomain)
			analyzer.ComputeCharacterV2(domainId, termFreq)
			fmt.Println("Analyzing Website Values ... ", domain.RootDomain)
			analyzer.ComputeValuesV2(domainId, termFreq)
			fmt.Println("Analyzing Website Cloud ...", domain.RootDomain)
			analyzer.ComputeWordcloudV2(domainId, termFreq)
		}()
	})

	/*******************
	 * v1 (lagaxy)     *
	 *******************/

	router.POST("/profiler/profiledomain", func(c *gin.Context) {

		c.Request.ParseForm()

		domain := c.Request.Form.Get("domain")
		domainId := c.Request.Form.Get("domainId")
		notCrawl := c.Request.Form.Get("notCrawl")
		weightedUrls := c.Request.Form["weightedUrls"]

		log.Printf("[POST] /profiler/profiledomain %s %s\n", domain, domainId)

		if domain == "" {
			c.String(http.StatusBadRequest, "FAIL")
		}
		if domainId == "" {
			c.String(http.StatusBadRequest, "FAIL")
		}
		seed := "http://" + domain

		go func() {

			resp, err := http.Head("http://" + domain)
			if err != nil {
				log.Printf("[Err] Head %s: %s", seed, err)
				return
			}
			seed := resp.Request.URL.String()

			domain, err := domainutil.ParseFromRawURL(seed)
			if err != nil {
				log.Printf("[Err] Failed to parse seed %s: %s", seed, err)
				return
			}

			//FIXME implement a single crawl method
			for _, weightedUrl := range weightedUrls {
				weightedUrl := strings.TrimRight(weightedUrl, "/")
				resp, err := http.Head(weightedUrl)
				if err != nil {
					log.Printf("[Err] Head %s: %s", seed, err)
					//return
					continue
				}
				u := resp.Request.URL.String()
				crawler.FullCrawl(domain, u, 1)
			}

			if len(notCrawl) == 0 {
				crawler.FullCrawl(domain, seed, 100)
			}
			//analyzer.Analyze(domainId, domain)

			log.Println("analyzing", domain.RootDomain)
			log.Println("computing character...")
			analyzer.ComputeCharacter(domainId, domain.RootDomain, weightedUrls)
			log.Println("computing value...")
			analyzer.ComputeValues(domainId, domain.RootDomain, weightedUrls)
			log.Println("computing wordcloud...")
			analyzer.ComputeWordCloud(domainId, domain.RootDomain, weightedUrls)
			log.Println("analyze done...")

		}()

		c.String(http.StatusOK, "OK")
	})

	router.GET("/profiler/twitter/:screenName/character", func(c *gin.Context) {
		screenName := c.Params.ByName("screenName")
		character, wordfrequency, _ := analyzer.ComputeTweetCharacter(screenName)
		c.JSON(http.StatusOK, gin.H{"character": character, "wordfrequency": wordfrequency})
	})

	router.GET("/profiler/test", func(c *gin.Context) {
		//analyzer.ComputeCharacter("", "anyperk.com")
		//analyzer.ComputeValues("anyperk.com")
		//analyzer.ComputeWordCloud("anyperk.com")
	})
	router.Run(":8080")
}
