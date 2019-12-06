package geometry

import (
	"fmt"

	"github.com/golang/geo/s2"
	geom "github.com/twpayne/go-geom"
)

func decodePoint(pointCoords geom.Coord) s2.Point {
	return s2.PointFromLatLng(
		s2.LatLngFromDegrees(pointCoords.Y(), pointCoords.X()))
}

func decodeLineString(lineStringCoords []geom.Coord) *s2.Polyline {
	points := make([]s2.Point, len(lineStringCoords))
	for i, pointCoords := range lineStringCoords {
		points[i] = decodePoint(pointCoords)
	}

	polyline := s2.Polyline(points)
	return &polyline
}

func decodeLinearRing(linearRingCoords []geom.Coord) *s2.Loop {
	if l := len(linearRingCoords) - 1; linearRingCoords[0].Equal(geom.XY, linearRingCoords[l]) {
		linearRingCoords = linearRingCoords[:l]
	}

	points := make([]s2.Point, len(linearRingCoords))
	for i, pointCoords := range linearRingCoords {
		points[i] = decodePoint(pointCoords)
	}

	loop := s2.LoopFromPoints(points)
	loop.Normalize()
	return loop
}

func decodePolygon(polygonCoords [][]geom.Coord) s2.Region {
	if len(polygonCoords) == 0 {
		return decodeLinearRing(polygonCoords[0])
	}

	loops := make([]*s2.Loop, 0, len(polygonCoords))
	for _, linearRingCoords := range polygonCoords {
		loops = append(loops, decodeLinearRing(linearRingCoords))
	}

	return s2.PolygonFromLoops(loops)
}

func decodeMultiPolygon(multiPolygonCoords [][][]geom.Coord) s2.Region {
	if len(multiPolygonCoords) == 1 {
		return decodePolygon(multiPolygonCoords[0])
	}

	l := 0
	for _, polygonCoords := range multiPolygonCoords {
		l += len(polygonCoords)
	}

	loops := make([]*s2.Loop, 0, l)
	for _, polygonCoords := range multiPolygonCoords {
		for _, linearRingCoords := range polygonCoords {
			loops = append(loops, decodeLinearRing(linearRingCoords))
		}
	}

	return s2.PolygonFromLoops(loops)
}

func RegionFromGeometry(geometryObject geom.T) (s2.Region, error) {
	switch g := geometryObject.(type) {
	case *geom.Point:
		return decodePoint(g.Coords()), nil
	case *geom.LineString:
		return decodeLineString(g.Coords()), nil
	case *geom.LinearRing:
		return decodeLinearRing(g.Coords()), nil
	case *geom.Polygon:
		return decodePolygon(g.Coords()), nil
	case *geom.MultiPolygon:
		return decodeMultiPolygon(g.Coords()), nil
	default:
		return nil, fmt.Errorf("no S2 equivalent implemented for geometry type %T", g)
	}
}
