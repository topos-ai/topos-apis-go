package scores

import (
	"context"
	"math"

	"github.com/topos-ai/topos-apis/genproto/go/topos/scores/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"

	"github.com/topos-ai/topos-apis-go/auth"
)

type Client struct {
	scoresClient scores.ScoresClient
}

func NewClient(addr string, secure bool) (*Client, error) {
	dialOptions := auth.DialOptions(secure)
	conn, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	scoresClient := scores.NewScoresClient(conn)
	c := &Client{
		scoresClient: scoresClient,
	}

	return c, nil
}

type Score struct {
	VertexA string  `json:"vertex_a,omitempty"`
	VertexB string  `json:"vertex_b,omitempty"`
	Score   float64 `json:"score"`
}

func (c *Client) BatchSetGraphScores(ctx context.Context, name string, batch []*Score) error {
	req := &scores.BatchSetGraphScoresRequest{
		Name:   name,
		Scores: make([]*scores.Score, len(batch)),
	}

	for i, score := range batch {
		req.Scores[i] = &scores.Score{
			VertexA: score.VertexA,
			VertexB: score.VertexB,
			Score:   score.Score,
		}
	}

	_, err := c.scoresClient.BatchSetGraphScores(ctx, req)
	return err
}

func (c *Client) TopGraphScores(ctx context.Context, name, vertexA string, pageSize int) ([]*Score, error) {
	req := &scores.TopGraphScoresRequest{
		Name:    name,
		VertexA: vertexA,
	}

	if pageSize > int(math.MaxInt32) {
		req.PageSize = math.MaxInt32
	} else {
		req.PageSize = int32(pageSize)
	}

	response, err := c.scoresClient.TopGraphScores(ctx, req)
	if err != nil {
		return nil, err
	}

	topScores := make([]*Score, len(response.Scores))
	for i, score := range response.Scores {
		topScores[i] = &Score{
			VertexB: score.VertexB,
			Score:   score.Score,
		}
	}

	return topScores, nil
}

func (c *Client) ListGraphScores(ctx context.Context, name, vertexA string) (*ScoreIterator, error) {
	it := &ScoreIterator{}
	fetch := func(pageSize int, pageToken string) (string, error) {
		req := &scores.ListGraphScoresRequest{
			Name:      name,
			PageToken: pageToken,
		}

		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}

		response, err := c.scoresClient.ListGraphScores(ctx, req)
		if err != nil {
			return "", err
		}

		if len(response.Scores) > cap(it.items)-len(it.items) {
			items := make([]*Score, len(it.items), len(it.items)+len(response.Scores))
			copy(items, it.items)
			it.items = items
		}

		for _, score := range response.Scores {
			it.items = append(it.items, &Score{
				VertexA: score.VertexA,
				VertexB: score.VertexB,
				Score:   score.Score,
			})
		}

		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}

type ScoreIterator struct {
	items    []*Score
	pageInfo *iterator.PageInfo
	nextFunc func() error
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package
// for details.
func (it *ScoreIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns Done, all subsequent calls will
// return Done.
func (it *ScoreIterator) Next() (*Score, error) {
	var item *Score
	if err := it.nextFunc(); err != nil {
		return item, err
	}

	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ScoreIterator) bufLen() int {
	return len(it.items)
}

func (it *ScoreIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
