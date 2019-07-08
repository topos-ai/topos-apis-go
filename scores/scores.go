package scores

import (
	"context"
	"math"

	"github.com/topos-ai/topos-apis/genproto/go/topos/scores/v1"
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
	Name    string  `json:"name,omitempty"`
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
