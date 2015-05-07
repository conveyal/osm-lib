package com.conveyal.osmlib;

import com.conveyal.osmlib.serializer.NodeSerializer;
import com.conveyal.osmlib.serializer.WaySerializer;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.NavigableSet;

/**
 * OTP representation of a subset of OpenStreetMap. One or more PBF files can be loaded into this
 * object, which serves as a simplistic database for fetching and iterating over OSM elements.
 * Using DB TreeMaps is often not any slower than memory. HashMaps are both bigger and slower.
 * This is probably because our keys are so small. A hashmap needs to store both the long key and its hash.
 *
 * FIXME rename this to OSMStorage or OSMDatabase
 */
public class OSM implements OSMEntitySink { // TODO implements OSMEntitySource, or make separate source/sink wrappers

    private static final Logger LOG = LoggerFactory.getLogger(OSM.class);

    public Map<Long, Node> nodes;
    public Map<Long, Way> ways;
    public Map<Long, Relation> relations;

    /** A tile-based spatial index. */
    public NavigableSet<Tuple3<Integer, Integer, Long>> index; // (x_tile, y_tile, wayId)

    /** The nodes that are referenced at least once by ways in this OSM. */
    NodeTracker referencedNodes = new NodeTracker();

    /** The nodes which are referenced more than once by ways in this OSM. */
    NodeTracker intersectionNodes = new NodeTracker();

    /** The MapDB backing this OSM, if any. */
    DB db = null;

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
        DBMaker dbMaker;
        if (diskPath == null) {
            LOG.info("OSM will be stored in a temporary file.");
            dbMaker = DBMaker.newTempFileDB().deleteFilesAfterClose();
        } else {
            if (diskPath.equals("__MEMORY__")) {
                LOG.info("OSM will be stored in memory.");
                // 'direct' means off-heap memory, no garbage collection overhead
                dbMaker = DBMaker.newMemoryDirectDB(); 
            } else {
                LOG.info("OSM will be stored in file {}.", diskPath);
                dbMaker = DBMaker.newFileDB(new File(diskPath));
            }
        }
        
        // Compression has no appreciable effect on speed but reduces file size by about 16 percent.
        // Hash table cache (eviction by collision) is on by default with a size of 32k records.
        // http://www.mapdb.org/doc/caches.html
        // Our objects do not have semantic hash and equals functions, but I suppose it's the table keys that are hashed
        // not the values.
        db = dbMaker.asyncWriteEnable()
                .transactionDisable()
                //.cacheDisable()
                .compressionEnable()
                .mmapFileEnableIfSupported()
                .closeOnJvmShutdown()
                .make();

        if (db.getAll().isEmpty()) {
            LOG.info("No OSM tables exist yet, they will be created.");
        }
        
        nodes = db.createTreeMap("nodes")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .valueSerializer(new NodeSerializer())
                .makeOrGet();
        
        ways =  db.createTreeMap("ways")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .valueSerializer(new WaySerializer())
                .makeOrGet();
                
        relations = db.createTreeMap("relations")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .makeOrGet();

        // Serializer delta-compresses the tuple as a whole and variable-width packs ints,
        // but does not recursively delta-code its elements.
        index = db.createTreeSet("spatial_index")
                .serializer(BTreeKeySerializer.TUPLE3) 
                .makeOrGet();

        db.createAtomicLong("timestamp", 0);

    }
    
    public void loadFromPBFFile (String filePath) {
        try {
            LOG.info("Reading PBF from file '{}'.", filePath);
            PBFInput pbfSource = new PBFInput(new FileInputStream(filePath), this);
            pbfSource.read();
        } catch (Exception ex) {
            LOG.error("Error occurred while parsing PBF file '{}'", filePath);
            ex.printStackTrace();
        }
    }

    // TODO readPbf, writePbf
    public void readVex(InputStream inputStream) {
        try {
            OSMEntitySource  source = new VexInput(inputStream, this);
            source.read();
        } catch (IOException ex) {
            LOG.error("Error occurred while parsing VEX stream.");
            ex.printStackTrace();
        }
    }

    /** Write the contents of this OSM MapDB out to a stream in VEX binary format. */
    public void writeVex(OutputStream outputStream) throws IOException {
        OSMEntitySink sink = new VexOutput(outputStream);
        this.writeToSink(sink);
    }

    /** Write the contents of this OSM MapDB out to an OSM entity sink. */
    public void writeToSink(OSMEntitySink sink) throws IOException {
        sink.writeBegin();
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

    /* OSM DATA SINK INTERFACE */

    @Override
    public void writeBegin() throws IOException {
        // Do nothing. Could initialize database here.
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
            long firstNodeId = way.nodes[0];
            Node firstNode = this.nodes.get(firstNodeId);
            if (firstNode == null) {
                LOG.warn("Leaving way {} out of the index. It references node {} that was not (yet) provided.", id, firstNodeId);
            } else {
                WebMercatorTile tile = new WebMercatorTile(firstNode.getLat(), firstNode.getLon());
                // We could also insert using ((float)lat, (float)lon) as a key
                // but depending on whether MapDB does tree path compression this might take more space
                this.index.add(new Tuple3(tile.xtile, tile.ytile, id));
            }
        }

    }

    @Override
    public void writeRelation(long id, Relation relation) {
        this.relations.put(id, relation);
    }

    @Override
    public void writeEnd() throws IOException {
        // Do nothing.
    }

}
