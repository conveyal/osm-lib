package com.conveyal.osmlib;

import com.conveyal.osmlib.serializer.NodeSerializer;
import com.conveyal.osmlib.serializer.WaySerializer;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.NavigableSet;

/**
 * osm-lib representation of a subset of OpenStreetMap. One or more OSM files (e.g. PBF) can be loaded into this
 * object, which serves as a simple in-process database for fetching and iterating over OSM elements.
 * Using DB TreeMaps is often not any slower than memory. HashMaps are both bigger and slower because our keys
 * are so small: a hashmap needs to store both the long key and its hash.
 *
 * FIXME rename this to OSMStorage or OSMDatabase
 */
public class OSM implements OSMEntitySource, OSMEntitySink {

    private static final Logger LOG = LoggerFactory.getLogger(OSM.class);

    public Map<Long, Node> nodes;
    public Map<Long, Way> ways;
    public Map<Long, Relation> relations;

    /** A tile-based spatial index.
     * Key is 16 bytes long, first 4byte integer x_tile, second 4 byte integer y_tile, third 8 byte wayId.
     * Use {@link OSM#indexMake(int, int, long)} to create new entry.
     * Use {@link OSM#getInt(byte[], int)} and {@link DataIO#getLong(byte[], int)} to read data
     * */
    public NavigableSet<byte[]> index; // (x_tile, y_tile, wayId)

    /** The nodes that are referenced at least once by ways in this OSM. */
    NodeTracker referencedNodes = new NodeTracker();

    /** The nodes which are referenced more than once by ways in this OSM. */
    public NodeTracker intersectionNodes = new NodeTracker();

    /** The MapDB backing this OSM, if any. */
    DB db = null;

    /** The timestamp in seconds since the Epoch of the last replication update applied. */
    Atomic.Long timestamp;

    /** The sequence number of the last replication patch applied. */
    Atomic.Long sequenceNumber;

    /* If true, insert all incoming ways in the index table. */
    public boolean tileIndexing = false;

    /* If true, track which nodes are referenced by more than one way. */
    public boolean intersectionDetection = false;

    /**
     * Construct a new MapDB-based random-access OSM data store.
     * If diskPath is null, OSM will be loaded into a temporary file and deleted on shutdown.
     * If diskPath is the string "__MEMORY__" the OSM will be stored entirely in memory.
     *
     * @param diskPath - the file in which to save the data, null for a temp file, or "__MEMORY__" for in-memory.
     */
    public OSM (String diskPath) {
        DBMaker.Maker dbMaker;
        if (diskPath == null) {
            LOG.info("OSM will be stored in a temporary file.");
            dbMaker = DBMaker.tempFileDB().deleteFilesAfterClose();
        } else {
            if (diskPath.equals("__MEMORY__")) {
                LOG.info("OSM will be stored in memory.");
                // 'direct' means off-heap memory, no garbage collection overhead
                dbMaker = DBMaker.memoryDirectDB();
            } else {
                LOG.info("OSM will be stored in file {}.", diskPath);
                dbMaker = DBMaker.fileDB(new File(diskPath));
            }
        }

        // Async write reduces write amplification and makes import faster

        // Compression has no appreciable effect on speed but reduces file size by about 16 percent.
        // Compression is applied to values only, keys are already compressed.

        // Hash table cache (eviction by collision) is on by default with a size of 32k records.
        // http://www.mapdb.org/doc/caches.html
        // Our objects do not have semantic hash and equals functions, but I suppose it's the table keys that are hashed
        // not the values.
        db = dbMaker
                .asyncWriteEnable()
                .transactionDisable()
                .fileMmapEnableIfSupported()
                .fileMmapCleanerHackEnable()
                .fileMmapPreclearDisable()
                .cacheHashTableEnable()
                .closeOnJvmShutdown()
                .make();

        if (db.getAll().isEmpty()) {
            LOG.info("No OSM tables exist yet, they will be created.");
        }

        nodes = db.treeMapCreate("nodes")
                .keySerializer(BTreeKeySerializer.LONG)
                .valueSerializer(new Serializer.CompressionWrapper(new NodeSerializer()))
                .makeOrGet();

        ways =  db.treeMapCreate("ways")
                .keySerializer(BTreeKeySerializer.LONG)
                .valueSerializer(new Serializer.CompressionWrapper(new WaySerializer()))
                .makeOrGet();

        relations = db.treeMapCreate("relations")
                .keySerializer(BTreeKeySerializer.LONG)
                .valueSerializer(new Serializer.CompressionWrapper(new Relation.RelationSerializer()))
                .makeOrGet();

        // Serializer delta-compresses the tuple as a whole and variable-width packs ints,
        // but does not recursively delta-code its elements.
        index = db.treeSetCreate("spatial_index")
                .serializer(BTreeKeySerializer.BYTE_ARRAY)
                .makeOrGet();

        // GetAtomicLong() will create the atomic long entry if it doesn't exist
        timestamp = db.atomicLong("timestamp");
        sequenceNumber = db.atomicLong("sequence_number");

    }

    // TODO put these read/write methods on all sources/sinks
    public void readFromFile(String filePath) {
        try {
            LOG.info("Reading OSM from file '{}'.", filePath);
            OSMEntitySource source = OSMEntitySource.forFile(filePath);
            source.copyTo(this);
        } catch (Exception ex) {
            throw new RuntimeException("Error occurred while parsing OSM file " + filePath, ex);
        }
    }

    public void readFromUrl(String urlString) {
        try {
            LOG.info("Reading OSM from URL '{}'.", urlString);
            OSMEntitySource source = OSMEntitySource.forUrl(urlString);
            source.copyTo(this);
        } catch (Exception ex) {
            throw new RuntimeException("Error occurred while parsing OSM from URL " + urlString, ex);
        }
    }

    public void writeToFile(String filePath) {
        try {
            LOG.info("Writing OSM to file '{}'.", filePath);
            OSMEntitySink sink = OSMEntitySink.forFile(filePath);
            this.copyTo(sink);
        } catch (Exception ex) {
            throw new RuntimeException("Error occurred while parsing OSM file " + filePath, ex);
        }
    }

    public void readVex(InputStream inputStream) {
        try {
            OSMEntitySource source = new VexInput(inputStream);
            source.copyTo(this);
        } catch (IOException ex) {
            LOG.error("Error occurred while parsing VEX stream.");
            ex.printStackTrace();
        }
    }

    public void readPbf(InputStream inputStream) {
        try {
            OSMEntitySource source = new PBFInput(inputStream);
            source.copyTo(this);
        } catch (IOException ex) {
            LOG.error("Error occurred while parsing VEX stream.");
            ex.printStackTrace();
        }
    }

    /** Write the contents of this OSM MapDB out to a stream in VEX binary format. */
    public void writeVex(OutputStream outputStream) throws IOException {
        OSMEntitySink sink = new VexOutput(outputStream);
        this.copyTo(sink);
    }

    /** Write the contents of this OSM MapDB out to a stream in PBF binary format. */
    public void writePbf(OutputStream outputStream) throws IOException {
        OSMEntitySink sink = new PBFOutput(outputStream);
        this.copyTo(sink);
    }

    /** Write the contents of this OSM MapDB out to an OSM entity sink (from OSMEntitySource interface). */
    @Override
    public void copyTo (OSMEntitySink sink) throws IOException {
        sink.setReplicationTimestamp(timestamp.get());
        sink.writeBegin();
        if (timestamp.get() > 0) {
            sink.setReplicationTimestamp(timestamp.get());
        }
        for (Map.Entry<Long, Node> nodeEntry : this.nodes.entrySet()) {
            sink.writeNode(nodeEntry.getKey(), nodeEntry.getValue());
        }
        for (Map.Entry<Long, Way> wayEntry : this.ways.entrySet()) {
            sink.writeWay(wayEntry.getKey(), wayEntry.getValue());
        }
        for (Map.Entry<Long, Relation> relationEntry : this.relations.entrySet()) {
            sink.writeRelation(relationEntry.getKey(), relationEntry.getValue());
        }
        sink.writeEnd();
    }


    /**
     * Insert the given way into the tile-based spatial index, based on its current node locations in the database.
     * If the way does not exist, this method does nothing (leaving any reference to the way in the index) because
     * it can't know anything about the location of a way that's already deleted. If the way object is not supplied
     * it will be looked up by its ID.
     */
    public void indexWay(long wayId, Way way) {
        // We could also insert using ((float)lat, (float)lon) as a key
        // but depending on whether MapDB does tree path compression this might take more space
        WebMercatorTile tile = tileForWay(wayId, way);
        if (tile == null) {
            LOG.debug("Attempted insert way {} into the spatial index, but it is not currently in the database.", wayId);
        } else {
            this.index.add(indexMake(tile.xtile, tile.ytile, wayId));
        }
    }

    public void unIndexWay(long wayId) {
        Way way = ways.get(wayId);
        if (way == null) {
            LOG.debug("Attempted to remove way {} from the spatial index, but it is not currently in the database.", wayId);
        } else {
            WebMercatorTile tile = tileForWay(wayId, way);
            if (tile != null) {
                this.index.remove(indexMake(tile.xtile, tile.ytile, wayId));
            }
        }
    }

    /** @return null if the way is not in the database and therefore can't be located. */
    private WebMercatorTile tileForWay (long wayId, Way way) {
        if (way == null) way = ways.get(wayId); // Way object was not supplied, fetch it from the database.
        if (way == null) return null; // Way does not exist anymore in the database, ignore it.
        long firstNodeId = way.nodes[0];
        Node firstNode = this.nodes.get(firstNodeId);
        if (firstNode == null) {
            LOG.debug("Leaving way {} out of the index. It references node {} that was not (yet) provided.",
                    wayId, firstNodeId);
            return null;
        } else {
            WebMercatorTile tile = new WebMercatorTile(firstNode.getLat(), firstNode.getLon());
            return tile;
        }
    }

    /* OSM DATA SINK INTERFACE */

    @Override
    public void writeBegin() throws IOException {
        // Do nothing. Could initialize database here.
        if ( ! (nodes.isEmpty() && ways.isEmpty() && relations.isEmpty())) {
            throw new RuntimeException("Database is already populated.");
        }
    }

    @Override
    public void setReplicationTimestamp(long secondsSinceEpoch) {
        // TODO handle the case where multiple files are loaded (oldest timestamp should be used)
        timestamp.set(secondsSinceEpoch);
    }

    @Override
    public void writeNode(long id, Node node) {
        this.nodes.put(id, node);
    }

    @Override
    public void writeWay(long id, Way way) {

        // Insert the way into the MapDB table.
        this.ways.put(id, way);

        // Optionally track which nodes are referenced by more than one way.
        if (intersectionDetection) {
            for (long nodeId : way.nodes) {
                if (referencedNodes.contains(nodeId)) {
                    intersectionNodes.add(nodeId);
                } else {
                    referencedNodes.add(nodeId);
                }
            }
        }

        // Insert the way into the tile-based spatial index according to its first node.
        if (tileIndexing) {
            indexWay(id, way);
        }

    }

    @Override
    public void writeRelation(long id, Relation relation) {
        this.relations.put(id, relation);
    }

    @Override
    public void writeEnd() throws IOException {
        db.commit();
        LOG.info("Compact started");
        db.compact();
        LOG.info("Compact finished");
    }

    /** Close the database file to ensure clean shutdown and avoid leaving the async write thread running. */
    public void close() {
        db.close();
    }

    public static int getInt(byte[] buf, int pos) {
        return
                (((int)buf[pos++] & 0xFF) << 24) |
                (((int)buf[pos++] & 0xFF) << 16) |
                (((int)buf[pos++] & 0xFF) <<  8) |
                (((int)buf[pos] & 0xFF));

    }

    public static void setInt(byte[] buf, int pos,int v) {
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos] = (byte) (0xff & (v));
    }

    public static byte[] indexMake(int x_tile, int y_tile, long wayId){
        if(wayId<0)
            throw new AssertionError("negative wayId not supported");
        //TODO clarify negative wayId, it does not work with byte[] comparator,
        // unsigned longs have negative values  higher

        byte[] ret = new byte[4+4+8];
        setInt(ret,0,x_tile);
        setInt(ret,4,y_tile);
        DataIO.putLong(ret,8,wayId);
        return ret;
    }
}
