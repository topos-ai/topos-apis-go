package points

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/topos-ai/topos-apis-go/auth"
	points "github.com/topos-ai/topos-apis/genproto/go/topos/points/v1"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"google.golang.org/api/iterator"
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

func (c *Client) PolygonSearchPoints(ctx context.Context, brand string, tags []string, polygon *geom.Polygon) (*PointIterator, error) {

	encodedGeometry, err := geojson.Marshal(polygon)
	if err != nil {
		return nil, err
	}

	it := &PointIterator{}
	fetch := func(pageSize int, pageToken string) (string, error) {
		client, err := c.pointsClient.PolygonSearchPoints(ctx)
		if err != nil {
			return "", err
		}

		req := &points.PolygonSearchPointsRequest{
			Brand:           brand,
			Tags:            tags,
			PolygonEncoding: points.PolygonEncoding_GEOJSON,

			PageToken: pageToken,
		}

		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
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
				return "", err
			}

			*req = points.PolygonSearchPointsRequest{}
		}

		response, err := client.CloseAndRecv()
		if err != nil {
			return "", err
		}

		if it.items == nil {
			it.items = response.Points
		} else {
			it.items = append(it.items, response.Points...)
		}

		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}

func (c *Client) GetBrand(ctx context.Context, brand string) (*points.Brand, error) {
	req := &points.GetBrandRequest{
		Name: brand,
	}

	return c.pointsClient.GetBrand(ctx, req)
}

type PointIterator struct {
	items    []*points.Point
	pageInfo *iterator.PageInfo
	nextFunc func() error
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package
// for details.
func (it *PointIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns Done, all subsequent calls will
// return Done.
func (it *PointIterator) Next() (*points.Point, error) {
	var item *points.Point
	if err := it.nextFunc(); err != nil {
		return item, err
	}

	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *PointIterator) bufLen() int {
	return len(it.items)
}

func (it *PointIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
