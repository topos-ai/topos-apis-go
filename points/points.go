package points

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/topos-ai/topos-apis-go/auth"
	points "github.com/topos-ai/topos-apis/genproto/go/topos/points/v1"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"google.golang.org/grpc"
)

type Client struct {
	pointsClient points.PointsClient
	conn         *grpc.ClientConn
}

func NewClient(addr string, secure bool) (*Client, error) {
	dialOptions := auth.DialOptions(secure, !strings.Contains(addr, "."))
	conn, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	pointsClient := points.NewPointsClient(conn)
	c := &Client{
		conn:         conn,
		pointsClient: pointsClient,
	}

	return c, nil
}

func (c *Client) CreatePoint(ctx context.Context, p *points.Point) (*points.Point, error) {
	req := &points.CreatePointRequest{
		Point: p,
	}

	return c.pointsClient.CreatePoint(ctx, req)
}

func (c *Client) Brand(ctx context.Context, name string) (*points.Brand, error) {
	req := &points.GetBrandRequest{
		Name: name,
	}

	return c.pointsClient.GetBrand(ctx, req)
}

func (c *Client) PolygonCountPoints(ctx context.Context, tags []string, polygon *geom.Polygon) (map[string]int64, error) {
	client, err := c.pointsClient.PolygonCountTagPoints(ctx)
	if err != nil {
		return nil, err
	}

	encodedGeometry, err := geojson.Marshal(polygon)
	if err != nil {
		return nil, err
	}

	req := &points.PolygonCountTagPointsRequest{
		Tags:            tags,
		PolygonEncoding: points.PolygonEncoding_GEOJSON,
	}

	for chunk := encodedGeometry; chunk != nil; {
		if len(chunk) > 1024 {
			req.PolygonChunk = chunk[:1024]
			chunk = chunk[1024:]
		} else {
			req.PolygonChunk = chunk
			chunk = nil
		}

		if err := client.Send(req); err != nil {
			return nil, err
		}
	}

	response, err := client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	json.NewEncoder(os.Stdout).Encode(response)
	fmt.Println(response)

	return response.TagPoints, nil
}

func (c *Client) PolygonSearchPoints(ctx context.Context, brand string, tags []string, polygon *geom.Polygon) (map[string]int64, error) {
	client, err := c.pointsClient.PolygonSearchPoints(ctx)
	if err != nil {
		return nil, err
	}

	encodedGeometry, err := geojson.Marshal(polygon)
	if err != nil {
		return nil, err
	}

	req := &points.PolygonSearchPointsRequest{
		Brand:           brand,
		Tags:            tags,
		PolygonEncoding: points.PolygonEncoding_GEOJSON,
		PageSize:        10,
	}

	for chunk := encodedGeometry; chunk != nil; {
		if len(chunk) > 1024 {
			req.PolygonChunk = chunk[:1024]
			chunk = chunk[1024:]
		} else {
			req.PolygonChunk = chunk
			chunk = nil
		}

		if err := client.Send(req); err != nil {
			return nil, err
		}
	}

	response, err := client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	json.NewEncoder(os.Stdout).Encode(response)
	fmt.Println(response)

	return nil, nil
}

func (c *Client) GetBrand(ctx context.Context, brand string) (*points.Brand, error) {
	req := &points.GetBrandRequest{
		Name: brand,
	}

	return c.pointsClient.GetBrand(ctx, req)
}
