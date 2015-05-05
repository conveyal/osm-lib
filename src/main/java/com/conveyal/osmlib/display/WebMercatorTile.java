package com.conveyal.osmlib.display;

import java.awt.geom.Rectangle2D;import java.lang.Math;

public class WebMercatorTile {

    // http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
    // http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

    public final int ZOOM = 14;

    public static int xTile(double lon, int zoom) {
        return (int) Math.floor((lon + 180) / 360 * (1 << zoom));
    }

    public static int yTile(double lat, int zoom) {
        return (int) Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat))
                + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1 << zoom));
    }

    public static Rectangle2D getRectangle (final int xTile, final int yTile, final int zoom) {
        double north = tile2lat(yTile, zoom);
        double south = tile2lat(yTile + 1, zoom);
        double west = tile2lon(xTile, zoom);
        double east = tile2lon(xTile + 1, zoom);
        return new Rectangle2D.Double(west, south, east - west, north - south);
    }

    public static double tile2lon(int xTile, int zoom) {
        return xTile / Math.pow(2.0, zoom) * 360.0 - 180;
    }

    public static double tile2lat(int yTile, int zoom) {
        double n = Math.PI - (2.0 * Math.PI * yTile) / Math.pow(2.0, zoom);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

}