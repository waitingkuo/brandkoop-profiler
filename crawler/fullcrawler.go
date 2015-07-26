package crawler

import (
	"github.com/PuerkitoBio/fetchbot"
	"github.com/PuerkitoBio/goquery"
	"github.com/waitingkuo/brandkoop-profiler/es"
	"github.com/waitingkuo/brandkoop-profiler/util"
	"github.com/waitingkuo/domainutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type FullCrawler struct {
	Mu            sync.Mutex
	Mux           *fetchbot.Mux
	Dup           map[string]bool
	Domain        *domainutil.Domain
	Seed          string
	Limit         int
	Count         int
	CrawlDelay    time.Duration
	WorkerIdleTTL time.Duration
}

func NewInstantCrawler(domain *domainutil.Domain, seed string, limit int) *FullCrawler {

	cw := &FullCrawler{
		Mux:           fetchbot.NewMux(),
		Dup:           make(map[string]bool),
		Domain:        domain,
		Seed:          seed,
		Limit:         limit,
		Count:         0,
		CrawlDelay:    time.Millisecond * 30,
		WorkerIdleTTL: time.Second * 3,
	}

	return cw
}

func NewFullCrawler(domain *domainutil.Domain, seed string, limit int) *FullCrawler {

	cw := &FullCrawler{
		Mux:           fetchbot.NewMux(),
		Dup:           make(map[string]bool),
		Domain:        domain,
		Seed:          seed,
		Limit:         limit,
		Count:         0,
		CrawlDelay:    time.Second * 1,
		WorkerIdleTTL: time.Second * 10,
	}

	return cw
}

func (cw *FullCrawler) Setup() {
	cw.Mux.Response().Method("GET").ContentType("text/html").Handler(fetchbot.HandlerFunc(
		func(ctx *fetchbot.Context, res *http.Response, err error) {

			doc, err := goquery.NewDocumentFromResponse(res)
			if err != nil {
				log.Printf("[Crawler] [ERR] %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
				return
			}

			// Extract Content
			content, err := util.ExtractContentFromDoc(ctx.Cmd.URL().String(), doc)
			if err != nil {
				log.Printf("[Crawler] [ERR] %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
				return
			}
			//println(content)
			err = es.IndexPage(cw.Domain, ctx.Cmd.URL().String(), content)
			if err != nil {
				log.Printf("[Crawler] [ERR] %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
				return
			}

			// Extract links
			links, err := util.ExtractLinksFromDoc(cw.Domain.RootDomain, ctx.Cmd.URL().String(), doc)
			if err != nil {
				log.Printf("[Crawler] [ERR] %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
				return
			}
			if cw.Count < cw.Limit {
				cw.Mu.Lock()
				for _, link := range links {

					if cw.Dup[link] == true {
						continue
					}

					if _, err := ctx.Q.SendStringGet(link); err != nil {
						log.Printf("[Crawler] [ERR] enqueue  %s - %s\n", link, err)
					} else {
						cw.Count += 1
						cw.Dup[link] = true
					}
					if cw.Count >= cw.Limit {
						break
					}
				}
				cw.Mu.Unlock()
			}

			/*
				type Page struct {
					Domain  string `json:"domain"`
					Url     string `json:"url"`
					Content string `json:"content"`
				}
			*/

		}))

}

func logHandler(wrapped fetchbot.Handler) fetchbot.Handler {
	return fetchbot.HandlerFunc(func(ctx *fetchbot.Context, res *http.Response, err error) {
		if err == nil {
			log.Printf("[%d] %s %s - %s\n", res.StatusCode, ctx.Cmd.Method(), ctx.Cmd.URL(), res.Header.Get("Content-Type"))
		}
		wrapped.Handle(ctx, res, err)
	})
}

func (cw *FullCrawler) Start() {

	h := logHandler(cw.Mux)

	f := fetchbot.New(h)
	f.CrawlDelay = cw.CrawlDelay
	f.WorkerIdleTTL = cw.WorkerIdleTTL
	f.AutoClose = true

	q := f.Start()

	cw.Dup[cw.Seed] = true
	_, err := q.SendStringGet(cw.Seed)
	if err != nil {
		log.Printf("[Crawler] [ERR] GET %s - %s\n", cw.Seed, err)
	}

	q.Block()
}
