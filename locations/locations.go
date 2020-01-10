package locations

import (
	"context"
	"io"
	"math"

	geometryproto "github.com/topos-ai/topos-apis/genproto/go/topos/geometry"
	"github.com/topos-ai/topos-apis/genproto/go/topos/locations/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"

	"github.com/topos-ai/topos-apis-go/auth"
)

type Client struct {
	locationsClient locations.LocationsClient
	conn            *grpc.ClientConn
}

func NewClient(addr string, useLocalCredentials bool) (*Client, error) {
	conn, err := auth.Dial(addr, useLocalCredentials)
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

func (c *Client) Region(ctx context.Context, region string) (*locations.Region, error) {
	req := &locations.GetRegionRequest{
		Name: region,
	}

	return c.locationsClient.GetRegion(ctx, req)
}

func (c *Client) RegionGeometry(ctx context.Context, w io.Writer, region string, encoding geometryproto.Encoding) error {
	req := &locations.GetRegionGeometryRequest{
		Name:             region,
		GeometryEncoding: encoding,
	}

	client, err := c.locationsClient.GetRegionGeometry(ctx, req)
	if err != nil {
		return err
	}

	for {
		response, err := client.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if _, err := w.Write(response.GeometryChunk); err != nil {
			return err
		}
	}
}

func (c *Client) SetRegion(ctx context.Context, region *locations.Region) error {
	req := &locations.SetRegionRequest{
		Region: region,
	}

	_, err := c.locationsClient.SetRegion(ctx, req)
	return err
}

func (c *Client) SetRegionGeometry(ctx context.Context, r io.Reader, name string, encoding geometryproto.Encoding) error {
	client, err := c.locationsClient.SetRegionGeometry(ctx)
	if err != nil {
		return err
	}

	req := &locations.SetRegionGeometryRequest{
		Name:             name,
		GeometryEncoding: encoding,
	}

	for chunk := make([]byte, 1024); ; {
		n, err := r.Read(chunk)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		req.GeometryChunk = chunk[:n]
		if err := client.Send(req); err != nil {
			return err
		}

		if n < 1024 {
			break
		}

		*req = locations.SetRegionGeometryRequest{}
	}

	return client.CloseSend()
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

func (c *Client) IntersectRegions(ctx context.Context, r io.Reader, regionType string) ([]*locations.IntersectRegionsResponse_IntersectingRegions, error) {
	client, err := c.locationsClient.IntersectRegions(ctx)
	if err != nil {
		return nil, err
	}

	req := &locations.IntersectRegionsRequest{
		RegionType:       regionType,
		GeometryEncoding: geometryproto.Encoding_WKB,
	}

	for chunk := make([]byte, 1024); ; {
		n, err := r.Read(chunk)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		req.GeometryChunk = chunk[:n]
		if err := client.Send(req); err != nil {
			return nil, err
		}

		if n < 1024 {
			break
		}

		*req = locations.IntersectRegionsRequest{}
	}

	response, err := client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return response.IntersectingRegions, nil
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
