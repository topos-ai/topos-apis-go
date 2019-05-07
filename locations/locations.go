package locations

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"

	"github.com/golang/geo/s2"
	"github.com/topos-ai/topos-apis/genproto/go/topos/locations/v1"
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
