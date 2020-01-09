package geometry

import (
	"encoding/binary"
	"encoding/json"

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
