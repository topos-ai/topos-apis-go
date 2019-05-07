package locations

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"math"

	"github.com/golang/geo/s2"
	"github.com/topos-ai/topos-apis/genproto/go/topos/locations/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	locationsClient locations.LocationsClient
	conn            *grpc.ClientConn
}

func NewClient(addr string, secure bool) (*Client, error) {
	dialOptions := []grpc.DialOption{}
	if secure {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

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

		for _, region := range response.Regions {
			it.items = append(it.items, region.Name)
		}

		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}
