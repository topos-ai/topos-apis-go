package scores

import (
	"context"
	"math"

	"github.com/topos-ai/topos-apis/genproto/go/topos/scores/v1"
	"google.golang.org/api/iterator"

	"github.com/topos-ai/topos-apis-go/auth"
)

type Client struct {
	scoresClient scores.ScoresClient
}

func NewClient(addr string, useLocalCredentials bool) (*Client, error) {
	conn, err := auth.Dial(addr, useLocalCredentials)
	if err != nil {
		return nil, err
	}

	scoresClient := scores.NewScoresClient(conn)
	c := &Client{
		scoresClient: scoresClient,
	}

	return c, nil
}

func (c *Client) SetGraphScore(ctx context.Context, name string, score *scores.Score) error {
	req := &scores.SetGraphScoreRequest{
		Name:  name,
		Score: score,
	}

	_, err := c.scoresClient.SetGraphScore(ctx, req)
	return err
}

func (c *Client) BatchSetGraphScores(ctx context.Context, name string, batch []*scores.Score) error {
	req := &scores.BatchSetGraphScoresRequest{
		Name:   name,
		Scores: batch,
	}

	_, err := c.scoresClient.BatchSetGraphScores(ctx, req)
	return err
}

func (c *Client) TopGraphScores(ctx context.Context, name, vertexA, vertexB string, pageSize int) ([]*scores.Score, error) {
	req := &scores.TopGraphScoresRequest{
		Name:           name,
		VertexA:        vertexA,
		VertexB:        vertexB,
		AscendingOrder: false,
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

	return response.Scores, nil
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

		if it.items == nil {
			it.items = response.Scores
		} else {
			it.items = append(it.items, response.Scores...)
		}

		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}

type ScoreIterator struct {
	items    []*scores.Score
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
func (it *ScoreIterator) Next() (*scores.Score, error) {
	var item *scores.Score
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
