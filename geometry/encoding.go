package geometry

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/topos-ai/topos-apis/genproto/go/topos/geometry"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/wkb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func decodeGeometry(data []byte, encoding geometry.Encoding) (geom.T, error) {
	switch encoding {
	case geometry.Encoding_WKB:
		return wkb.Unmarshal(data)

	case geometry.Encoding_GEOJSON:
		gg := &geojson.Geometry{}
		if err := json.Unmarshal(data, gg); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid geojson")
		}

		return gg.Decode()

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown geometry encoding")
	}
}

func RecvGeometry(encoding geometry.Encoding, chunk []byte, next func() ([]byte, error)) (geom.T, error) {
	if len(chunk) > 1024 {
		return nil, status.Error(codes.InvalidArgument, "geometry chunk longer than 1024 bytes")
	}

	buffer := bytes.NewBuffer(make([]byte, 0, len(chunk)))
	if _, err := buffer.Write(chunk); err != nil {
		return nil, err
	}

	for {
		chunk, err := next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if len(chunk) > 1024 {
			return nil, status.Error(codes.InvalidArgument, "geometry chunk longer than 1024 bytes")
		}

		if _, err := buffer.Write(chunk); err != nil {
			return nil, err
		}
	}

	return decodeGeometry(buffer.Bytes(), encoding)
}

func SendGeometry(encoding geometry.Encoding, geometryObject geom.T, send func(chunk []byte) error) error {
	var chunk []byte
	switch encoding {
	case geometry.Encoding_WKB:
		wkbBytes, err := wkb.Marshal(geometryObject, binary.BigEndian)
		if err != nil {
			return err
		}

		chunk = wkbBytes

	case geometry.Encoding_GEOJSON:
		geojsonBytes, err := geojson.Marshal(geometryObject)
		if err != nil {
			return err
		}

		chunk = geojsonBytes

	default:
		return status.Error(codes.InvalidArgument, "unknown geometry encoding")
	}

	for len(chunk) > 1024 {
		if err := send(chunk[:1024]); err != nil {
			return err
		}

		chunk = chunk[1024:]
	}

	return send(chunk)
}
