package main

import (
	"github.com/gin-gonic/gin"
	//"net/url"
	"github.com/waitingkuo/brandkoop-profiler/analyzer"
	"github.com/waitingkuo/brandkoop-profiler/crawler"
	"github.com/waitingkuo/domainutil"
	"log"
	"net/http"
)

func main() {

	router := gin.Default()
	router.POST("/profiler/profiledomain", func(c *gin.Context) {

		c.Request.ParseForm()

		domain := c.Request.Form.Get("domain")
		domainId := c.Request.Form.Get("domainId")
		notCrawl := c.Request.Form.Get("notCrawl")

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

			if len(notCrawl) == 0 {
				crawler.FullCrawl(domain, seed, 100)
			}
			//analyzer.Analyze(domainId, domain)

			log.Println("analyzing", domain.RootDomain)
			log.Println("computing character...")
			analyzer.ComputeCharacter(domainId, domain.RootDomain)
			log.Println("computing value...")
			analyzer.ComputeValues(domainId, domain.RootDomain)
			log.Println("computing wordcloud...")
			analyzer.ComputeWordCloud(domainId, domain.RootDomain)
			log.Println("analyze done...")

		}()

		c.String(http.StatusOK, "OK")
	})

	router.GET("/profiler/test", func(c *gin.Context) {
		//analyzer.ComputeCharacter("", "anyperk.com")
		//analyzer.ComputeValues("anyperk.com")
		//analyzer.ComputeWordCloud("anyperk.com")
	})
	router.Run(":8080")
}
