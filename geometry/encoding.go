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

func Marshal(geometryObject geom.T, encoding geometry.Encoding) ([]byte, error) {
	switch encoding {
	case geometry.Encoding_WKB:
		return wkb.Marshal(geometryObject, binary.BigEndian)
	case geometry.Encoding_GEOJSON:
		return geojson.Marshal(geometryObject)
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown geometry encoding")
	}
}

func Unmarshal(data []byte, encoding geometry.Encoding) (geom.T, error) {
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

func SendGeometry(r io.Reader, send func([]byte) error) error {
	chunk := make([]byte, 1024)
	for {
		n, err := r.Read(chunk)
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		nextChunk := chunk[:n]
		if err := send(nextChunk); err != nil {
			return err
		}
	}
}

func SendGeometryBytes(data []byte, send func([]byte) error) error {
	return SendGeometry(bytes.NewReader(data), send)
}

func RecvGeometry(w io.Writer, recv func() ([]byte, error)) error {
	for {
		chunk, err := recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
}

func RecvGeometryBytes(chunk []byte, recv func() ([]byte, error)) ([]byte, error) {
	buffer := bytes.NewBuffer(chunk)
	if err := RecvGeometry(buffer, recv); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
