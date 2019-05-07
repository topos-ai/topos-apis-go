package topiary

import (
	"bytes"
	"context"
	"crypto/tls"
	"math"

	"github.com/golang/geo/s2"
	"github.com/topos-ai/topos-apis/genproto/go/topos/topiary/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	topiaryClient topiary.TopiaryClient
	conn          *grpc.ClientConn
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

	topiaryClient := topiary.NewTopiaryClient(conn)
	c := &Client{
		conn:          conn,
		topiaryClient: topiaryClient,
	}

	return c, nil
}

func (c *Client) SetIDPosition(ctx context.Context, id []byte, position s2.CellID) error {
	req := &topiary.SetIDPositionRequest{
		Id:       id,
		Position: uint64(position),
	}

	_, err := c.topiaryClient.SetIDPosition(ctx, req)
	return err
}

func (c *Client) SetIDKeyValues(ctx context.Context, id []byte, key string, values []string, clear bool) error {
	req := &topiary.SetIDKeyValueRequest{
		Id:     id,
		Key:    key,
		Values: values,
		Clear:  clear,
	}

	_, err := c.topiaryClient.SetIDKeyValue(ctx, req)
	return err
}

type IDIterator struct {
	items    [][]byte
	pageInfo *iterator.PageInfo
	nextFunc func() error
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package
// for details.
func (it *IDIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns Done, all subsequent calls will
// return Done.
func (it *IDIterator) Next() ([]byte, error) {
	var item []byte
	if err := it.nextFunc(); err != nil {
		return item, err
	}

	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *IDIterator) bufLen() int {
	return len(it.items)
}

func (it *IDIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

func (c *Client) SearchIDs(ctx context.Context, keyValuePairs [][2]string, polygon *s2.Polygon) (*IDIterator, error) {
	polygonBuffer := bytes.NewBuffer([]byte{})
	if err := polygon.Encode(polygonBuffer); err != nil {
		return nil, err
	}

	topiaryKeyValuePairs := make([]*topiary.KeyValuePair, len(keyValuePairs))
	for i, keyValuePair := range keyValuePairs {
		topiaryKeyValuePairs[i] = &topiary.KeyValuePair{
			Key:   keyValuePair[0],
			Value: keyValuePair[1],
		}
	}

	it := &IDIterator{}
	polygonBytes := polygonBuffer.Bytes()
	fetch := func(pageSize int, pageToken string) (string, error) {
		client, err := c.topiaryClient.SearchIDs(ctx)
		if err != nil {
			return "", err
		}

		req := &topiary.SearchIDsRequest{
			PageToken:     pageToken,
			KeyValuePairs: topiaryKeyValuePairs,
			PolygonLength: int64(len(polygonBytes)),
		}

		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}

		if err := client.Send(req); err != nil {
			return "", err
		}

		*req = topiary.SearchIDsRequest{}
		for chunk := polygonBytes; chunk != nil; {
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
		}

		response, err := client.CloseAndRecv()
		if err != nil {
			return "", err
		}

		it.items = append(it.items, response.Ids...)
		return response.NextPageToken, nil
	}

	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = 1024
	return it, nil
}

func (c *Client) CountIDs(ctx context.Context, key string, polygon *s2.Polygon) (map[string]int64, error) {
	polygonBuffer := bytes.NewBuffer([]byte{})
	if err := polygon.Encode(polygonBuffer); err != nil {
		return nil, err
	}

	polygonBytes := polygonBuffer.Bytes()
	client, err := c.topiaryClient.CountIDs(ctx)
	if err != nil {
		return nil, err
	}

	req := &topiary.CountIDsRequest{
		Key:           key,
		PolygonLength: int64(len(polygonBytes)),
	}

	if err := client.Send(req); err != nil {
		return nil, err
	}

	*req = topiary.CountIDsRequest{}
	for chunk := polygonBytes; chunk != nil; {
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

	return response.ValueCounts, nil
}
