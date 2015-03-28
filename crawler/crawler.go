package crawler

import "github.com/waitingkuo/domainutil"

func FullCrawl(domain *domainutil.Domain, seed string, limit int) {
	cw := NewFullCrawler(domain, seed, limit)
	cw.Setup()
	cw.Start()
}
