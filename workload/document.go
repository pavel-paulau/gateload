package workload

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/couchbaselabs/gateload/api"
)

type DocSizeDistributionElement struct {
	Prob    int
	MinSize int
	MaxSize int
}

type DocSizeDistribution []*DocSizeDistributionElement

type DocSizeGenerator struct {
	cutoffs []int
	dist    DocSizeDistribution
}

func NewDocSizeGenerator(dist DocSizeDistribution) (*DocSizeGenerator, error) {
	rv := DocSizeGenerator{
		dist:    dist,
		cutoffs: make([]int, len(dist)),
	}

	var total int = 0

	for i, distelem := range dist {
		rv.cutoffs[i] = total + distelem.Prob - 1
		total += distelem.Prob
	}
	if total != 100 {
		return nil, fmt.Errorf("document distribution probabilities must sum to 100")
	}
	return &rv, nil
}

func (dsg *DocSizeGenerator) NextDocSize() int {

	whichDist := int(rand.Int31n(100))
	for i, cutoff := range dsg.cutoffs {
		if whichDist <= cutoff {
			dist := dsg.dist[i]
			return int(rand.Float64()*float64(dist.MaxSize-dist.MinSize)) + dist.MinSize
		}
	}

	return 0
}

func DocIterator(start, end int, dsg *DocSizeGenerator, channel string, sendAttachment bool) <-chan api.Doc {
	ch := make(chan api.Doc)
	go func() {
		for i := start; i < end; i++ {
			docid := Hash(strconv.FormatInt(int64(i), 10))
			rev := Hash(strconv.FormatInt(int64(i*i), 10))
			if sendAttachment {
				att := []byte(RandString(docid, dsg.NextDocSize()))
				attachmentContent := api.AttachmentContent{
					Data: base64.StdEncoding.EncodeToString(att),
				}
				//Use doc_id as name of attachment to ensure it is unique
				attachment := make(map[string]api.AttachmentContent)
				attachment[docid] = attachmentContent

				doc := api.Doc{
					Id:          docid,
					Rev:         fmt.Sprintf("1-%s", rev),
					Channels:    []string{channel},
					Revisions:   map[string]interface{}{"ids": []string{rev}, "start": 1},
					Attachments: map[string]api.AttachmentContent{docid: attachmentContent},
				}
				ch <- doc
			} else {
				doc := api.Doc{
					Id:        docid,
					Rev:       fmt.Sprintf("1-%s", rev),
					Channels:  []string{channel},
					Data:      map[string]string{docid: RandString(docid, dsg.NextDocSize())},
					Revisions: map[string]interface{}{"ids": []string{rev}, "start": 1},
				}
				ch <- doc
			}
		}
		close(ch)
	}()
	return ch
}
