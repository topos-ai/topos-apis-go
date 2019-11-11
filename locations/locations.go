package locations

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/golang/geo/s2"
	"github.com/topos-ai/topos-apis/genproto/go/topos/locations/v1"
	geom "github.com/twpayne/go-geom"
	geojson "github.com/twpayne/go-geom/encoding/geojson"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"

	"github.com/topos-ai/topos-apis-go/auth"
)

type Client struct {
	locationsClient locations.LocationsClient
	conn            *grpc.ClientConn
}

func NewClient(addr string, secure bool) (*Client, error) {
	dialOptions := auth.DialOptions(secure, !strings.Contains(addr, "."))
	conn, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	locationsClient := locations.NewLocationsClient(conn)
	c := &Client{
		conn:            conn,
		locationsClient: locationsClient,
	}

	return c, nil
}

func (c *Client) RegionGeometry(ctx context.Context, region string) (*s2.Polygon, error) {
	req := &locations.GetRegionGeometryRequest{
		Name: region,
	}

	client, err := c.locationsClient.GetRegionGeometry(ctx, req)
	if err != nil {
		return nil, err
	}

	polygonBuffer := bytes.NewBuffer([]byte{})
	for {
		response, err := client.Recv()
		if err == io.EOF {
			break
		}

		if _, err := polygonBuffer.Write(response.PolygonChunk); err != nil {
			return nil, err
		}
	}

	polygon := &s2.Polygon{}
	if err := polygon.Decode(polygonBuffer); err != nil {
		return nil, err
	}

	return polygon, nil
}

func (c *Client) LocateRegions(ctx context.Context, regionType string, latitude, longitude float64) ([]string, error) {
	req := &locations.LocateRegionsRequest{
		RegionType: regionType,
		Location: &locations.LatLng{
			Latitude:  latitude,
			Longitude: longitude,
		},
	}

	response, err := c.locationsClient.LocateRegions(ctx, req)
	if err != nil {
		return nil, err
	}

	return response.Regions, nil
}

func (c *Client) GetRegionGeometry(ctx context.Context, name string) (*s2.Polygon, error) {
	req := &locations.GetRegionGeometryRequest{
		Name: name,
	}

	client, err := c.locationsClient.GetRegionGeometry(ctx, req)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	for {
		response, err := client.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if _, err := buffer.Write(response.PolygonChunk); err != nil {
			return nil, err
		}
	}

	polygon := &s2.Polygon{}
	if err := polygon.Decode(buffer); err != nil {
		return nil, err
	}

	return polygon, nil
}

func (c *Client) SetRegion(ctx context.Context, region *locations.Region) error {
	req := &locations.SetRegionRequest{
		Region: region,
	}

	_, err := c.locationsClient.SetRegion(ctx, req)
	return err
}

func (c *Client) SetRegionGeometryGeoJSON(ctx context.Context, name string, geojsonString []byte) error {
	client, err := c.locationsClient.SetRegionGeometry(ctx)
	if err != nil {
		return err
	}

	req := &locations.SetRegionGeometryRequest{
		Name:             name,
		GeometryEncoding: locations.GeometryEncoding_GEOJSON,
	}

	for geojsonString != nil {
		if len(geojsonString) <= 1024 {
			req.GeometryChunk = geojsonString
			geojsonString = nil
		} else {
			req.GeometryChunk = geojsonString[:1024]
			geojsonString = geojsonString[1024:]
		}

		if err := client.Send(req); err != nil {
			return err
		}

		req.Reset()
	}

	_, err = client.CloseAndRecv()
	return err
}

type RegionIterator struct {
	items    []string
	pageInfo *iterator.PageInfo
	nextFunc func() error
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package
// for details.
func (it *RegionIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns Done, all subsequent calls will
// return Done.
func (it *RegionIterator) Next() (string, error) {
	var item string
	if err := it.nextFunc(); err != nil {
		return item, err
	}

	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *RegionIterator) bufLen() int {
	return len(it.items)
}

func (it *RegionIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

type SearchRegionOption func(*locations.SearchRegionsRequest)

func SearchRegionsByRegionType(regionType string) SearchRegionOption {
	return func(req *locations.SearchRegionsRequest) {
		req.RegionType = regionType
	}
}

func SearchRegionsByIncludingRegion(name string) SearchRegionOption {
	return func(req *locations.SearchRegionsRequest) {
		req.IncludedByRegion = name
	}
}

func (c *Client) SearchRegions(ctx context.Context, options ...SearchRegionOption) (*RegionIterator, error) {
	it := &RegionIterator{}
	fetch := func(pageSize int, pageToken string) (string, error) {
		req := &locations.SearchRegionsRequest{
			PageToken:       pageToken,
			ExcludeGeometry: true,
		}

		for _, option := range options {
			option(req)
		}

		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}

		response, err := c.locationsClient.SearchRegions(ctx, req)
		if err != nil {
			return "", err
		}

		if len(response.Regions) > cap(it.items)-len(it.items) {
			items := make([]string, len(it.items), len(it.items)+len(response.Regions))
			copy(items, it.items)
			it.items = items
		}

		for _, region := range response.Regions {
			it.items = append(it.items, region.Name)
		}

		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}

type IntersectingRegion struct {
	Name string
	Area float64
}

func (c *Client) IntersectRegions(ctx context.Context, regionType string, geometry geom.T) ([]*IntersectingRegion, error) {
	encodedGeometry, err := geojson.Marshal(geometry)
	if err != nil {
		return nil, err
	}

	client, err := c.locationsClient.IntersectRegions(ctx)
	if err != nil {
		return nil, err
	}

	req := &locations.IntersectRegionsRequest{
		RegionType:       regionType,
		GeometryEncoding: locations.GeometryEncoding_GEOJSON,
	}

	for chunk := encodedGeometry; chunk != nil; {
		if len(chunk) > 1024 {
			req.GeometryChunk = chunk[:1024]
			chunk = chunk[1024:]
		} else {
			req.GeometryChunk = chunk
			chunk = nil
		}

		if err := client.Send(req); err != nil {
			return nil, err
		}

		*req = locations.IntersectRegionsRequest{}
	}

	response, err := client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	for _, e := range response.IntersectingRegions {
		fmt.Println(e)
	}

	return nil, nil

}
