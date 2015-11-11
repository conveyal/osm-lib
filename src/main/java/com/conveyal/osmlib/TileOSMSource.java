package com.conveyal.osmlib;

import org.mapdb.DataIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/** An OSM source that pulls web Mercator tiles out of a disk-backed OSM store. */
public class TileOSMSource implements OSMEntitySource {

    protected static final Logger LOG = LoggerFactory.getLogger(TileOSMSource.class);

    private int minX, minY, maxX, maxY;

    private OSM osm;

    public TileOSMSource (OSM osm) {
        this.osm = osm;
    }

    public void setTileRange(int minX, int minY, int maxX, int maxY) {
        if (minX > maxX || minY > maxY) {
            throw new IllegalArgumentException("Min must be smaller or equal to max.");
        }
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
    }

    public void setBoundingBox(double minLat, double minLon, double maxLat, double maxLon) {
        WebMercatorTile minTile = new WebMercatorTile(minLat, minLon);
        WebMercatorTile maxTile = new WebMercatorTile(maxLat, maxLon);
        // Note that y tile numbers are increasing in the opposite direction of latitude (from north to south)
        // so the parameter order min,max.max,min is intentional.
        setTileRange(minTile.xtile, maxTile.ytile, maxTile.xtile, minTile.ytile);

    }

    public void copyTo (OSMEntitySink sink) {
        // Avoid writing out shared/intersection nodes more than once. Besides being wasteful, the first node in one way
        // may be the last node in the previous way output, which would create a node ID delta of zero and prematirely
        // end the block.
        NodeTracker nodesSeen = new NodeTracker();
        try {
            sink.writeBegin();
            for (int pass = 0; pass < 2; pass++) {
                for (int x = minX; x <= maxX; x++) {
                    // SortedSet provides one-dimensional ordering and iteration. Tuple3 gives an odometer-like ordering.
                    // Therefore we must vary one of the dimensions "manually". Consider a set containing all the
                    // integers from 00 to 99 at 2-tuples. The range from (1,1) to (2,2) does not contain the four
                    // elements (1,1) (1,2) (2,1) (2,2). It contains the elements (1,1) (1,2) (1,3) (1,4) ... (2,2).

                    //TODO this can be even more speedup by using iterator, and cutting iteration when maxY is too high
                    Set<byte[]> xSubset = osm.index.subSet(
                            OSM.indexMake(x, minY,0),   //inclusive lower bound, 0 is minimal LONG value
                            OSM.indexMake(x, maxY+1, 0) //exclusive upper bound, increase maxY by one
                    );
                    for (byte[] item : xSubset) {
                        long wayId = DataIO.getLong(item,8);
                        Way way = osm.ways.get(wayId);
                        if (way == null) {
                            LOG.error("Way {} is not available.", wayId);
                            continue;
                        }
                        if (pass == 0) { // Nodes
                            for (long nodeId : way.nodes) {
                                if (nodesSeen.contains(nodeId)) continue;
                                Node node = osm.nodes.get(nodeId);
                                if (node == null) {
                                    LOG.error("Way references a node {} that was not loaded.", nodeId);
                                } else {
                                    sink.writeNode(nodeId, node);
                                    nodesSeen.add(nodeId);
                                }
                            }
                        } else if (pass == 1) {
                            sink.writeWay(wayId, way);
                        }
                    }
                }
            }
            sink.writeEnd();
        } catch (IOException ex) {
            throw new RuntimeException("I/O exception while writing tiled OSM data.", ex);
        }
    }

}
