package util

import (
	"crypto/sha1"
	"encoding/base64"
	"github.com/PuerkitoBio/goquery"
	//"github.com/advancedlogic/GoOse"
	"github.com/marketmuse/GoOse"
	"github.com/waitingkuo/domainutil"
	"log"
	"math/rand"
	"net/url"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func init() {
	rand.Seed(time.Now().Unix())
}

func MeteorId() string {
	b := make([]rune, 17)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

var contentExtractor goose.Goose

func init() {
	contentExtractor = goose.New()
}

func Hash(s string) string {
	sha1Result := sha1.Sum([]byte(s))
	return base64.URLEncoding.EncodeToString(sha1Result[:])
}

func ExtractContentFromDoc(url string, doc *goquery.Document) (string, error) {

	rawHtml, err := doc.Html()
	if err != nil {
		return "", err
	}
	article := contentExtractor.ExtractFromRawHtml(url, rawHtml)

	return article.CleanedText, nil
}

// only fetch the links with the same rootdomain as domain
func ExtractLinksFromDoc(rootDomain string, currentLink string, doc *goquery.Document) ([]string, error) {

	links := []string{}
	u, err := url.Parse(currentLink)
	if err != nil {
		log.Printf("[Util] [ERR]: resolve URL %s - %s\n", currentLink, err)
		return []string{}, err
	}

	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		link, _ := s.Attr("href")
		//log.Println("[ExtractLinksFromDoc] found link", link)

		newURL, err := u.Parse(link)
		if err != nil {
			log.Printf("[Util] [ERR]: failed to parse URL %s - %s\n", link, err)
			return
		}

		domain, err := domainutil.ParseFromURL(newURL)
		if err != nil {
			log.Printf("[Util] [ERR]: Failed to parse  %s - %s\n", newURL.String(), err)
			return
		}

		if domain.RootDomain != rootDomain {
			//		log.Printf("not in the same domain %s %s", domain.RootDomain, rootDomain)
			return
		}

		links = append(links, newURL.String())
	})

	return links, nil
}
