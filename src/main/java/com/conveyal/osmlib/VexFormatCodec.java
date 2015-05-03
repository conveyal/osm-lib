package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Decode (or encode) a stream of VEX data into an OTP OSM data store.
 * This is a sort of data pump between an OTP OSM store and and InputStream / OutputStream.
 * Neither threadsafe nor reentrant! Create one new instance of the codec per encode/decode operation.
 */
public class VexFormatCodec {

    private static final Logger LOG = LoggerFactory.getLogger(VexFormatCodec.class);

    public static final String HEADER = "VEXFMT";
    public static final int VEX_NODE = 0;
    public static final int VEX_WAY = 1;
    public static final int VEX_RELATION = 2;
    public static final int VEX_NONE = 3;

    /* The input stream providing decompressed VEX format. */
    private VarIntInputStream vin;

    /* The output sink for uncompressed VEX format. */
    private VarIntOutputStream vout;

    /* Persistent values for delta coding. */
    private long id, ref, prevId, prevRef, prevFixedLat, prevFixedLon;
    private double lat, lon;

    /* The OSM data store to stuff the elements into. */
    private OSM osm;

    /* Writers produce the format, readers consume it. */
    public void writeVex(OSM osm, OutputStream outputStream) throws IOException {
        LOG.info("Writing VEX format...");
        this.osm = osm;
        GZIPOutputStream gzOut = new GZIPOutputStream(outputStream);
        vout = new VarIntOutputStream(gzOut);
        vout.writeBytes(HEADER.getBytes());
        long nEntities = 0;
        nEntities += writeNodeBlock();
        nEntities += writeWayBlock();
        nEntities += writeRelationBlock();
        // Empty block of type NONE indicates end of blocks
        beginWriteBlock(VEX_NONE);
        // Write the total number of blocks including terminator as a "checksum"
        // (should this be total number of entities written instead?)
        endWriteBlock(4);
        gzOut.close(); // also finishes writing compressed data before closing the underlying stream;
        LOG.info("Done writing VEX format.");
    }

    public void readVex(InputStream gzVexStream, OSM osm) throws IOException {
        LOG.info("Reading VEX format...");
        this.vin = new VarIntInputStream(new GZIPInputStream(gzVexStream));
        this.osm = osm;
        byte[] header = vin.readBytes(HEADER.length());
        if ( ! Arrays.equals(header, HEADER.getBytes())) {
            throw new IOException("Corrupt header.");
        }
        boolean done = false;
        long nBlocks = 0;
        while ( ! done) {
            LOG.info("reading block {}", nBlocks);
            done = readBlock();
            nBlocks += 1;
        }
        long sentinel = vin.readSInt64();
        long expectedBlocks = vin.readUInt64();
        if (sentinel != 0) {
            LOG.error("End block should not contain any elements.");
        }
        if (nBlocks != expectedBlocks) {
            LOG.error("Did not read the expected number of blocks.");
        }
        LOG.info("Done reading.");
    }

    private void beginWriteBlock(int etype) throws IOException {
        prevId = prevRef = 0;
        prevFixedLat = prevFixedLon = 0;
        vout.writeUInt32(etype);
    }

    /* @param n - the number of entities that were written in this block. */
    private void endWriteBlock(int n) throws IOException {
        vout.writeSInt64(0L);
        vout.writeUInt32(n);
    }

    /**
     * Note that the MapDB TreeMap is ordered, so we are writing out the nodes in ID order.
     * This should be good for compression.
     */
    private int writeNodeBlock() throws IOException {
        beginWriteBlock(VEX_NODE);
        int n = 0;
        for (Map.Entry<Long, Node> entry : osm.nodes.entrySet()) {
            writeNode(entry.getKey(), entry.getValue());
            n++;
        }
        endWriteBlock(n);
        return n;
    }

    private int writeWayBlock() throws IOException {
        beginWriteBlock(VEX_WAY);
        int n = 0;
        for (Map.Entry<Long, Way> entry : osm.ways.entrySet()) {
            writeWay(entry.getKey(), entry.getValue());
            n++;
        }
        endWriteBlock(n);
        return n;
    }

    private int writeRelationBlock() throws IOException {
        beginWriteBlock(VEX_RELATION);
        int n = 0;
        for (Map.Entry<Long, Relation> entry : osm.relations.entrySet()) {
            writeRelation(entry.getKey(), entry.getValue());
            n++;
        }
        endWriteBlock(n);
        return n;
    }

    /** Write the first elements common to all OSM entities: ID and tags. */
    private void writeCommonFields(long id, OSMEntity osmEntity) throws IOException {
        vout.writeSInt64(id - prevId);
        prevId = id;
        writeTags(osmEntity);
    }

    private void writeNode(long id, Node node) throws IOException {
        writeCommonFields(id, node);
        // plain ints should be fine rather than longs:
        // 2**31 = 2147483648
        // 180e7 = 1800000000.0
        long fixedLat = (long) (node.fixedLat);
        long fixedLon = (long) (node.fixedLon);
        vout.writeSInt64(fixedLat - prevFixedLat);
        vout.writeSInt64(fixedLon - prevFixedLon);
        prevFixedLat = fixedLat;
        prevFixedLon = fixedLon;
    }

    private void writeWay(long id, Way way) throws IOException {
        writeCommonFields(id, way);
        vout.writeUInt32(way.nodes.length);
        for (long ref : way.nodes) {
            vout.writeSInt64(ref - prevRef);
            prevRef = ref;
        }
    }

    private void writeRelation(long id, Relation relation) throws IOException {
        writeCommonFields(id, relation);
        vout.writeUInt32(relation.members.size());
        for (Relation.Member member : relation.members) {
            vout.writeSInt64(member.id);
            vout.writeUInt32(member.type.ordinal()); // FIXME bad, assign specific numbers
            vout.writeString(member.role);
        }
    }

    private static OSMEntity.Type[] typeForOrdinal = OSMEntity.Type.values();

    public boolean readRelation() throws IOException {
        /* Create a new instance each time because we don't know if this is going in a MapDB or a normal Map. */
        Relation relation = new Relation();
        long idDelta = vin.readSInt64();
        if (idDelta == 0) return true;
        id += idDelta;
        relation.tags = readTags();
        int nMembers = vin.readUInt32();
        for (int i = 0; i < nMembers; i++) {
            Relation.Member member = new Relation.Member();
            member.id = vin.readSInt64();
            member.type = typeForOrdinal[vin.readUInt32()]; // FIXME bad, assign specific numbers
            member.role = vin.readString();
            relation.members.add(member);
        }
        osm.relations.put(id, relation);
        //System.out.println(id + " " + relation.toString());
        return false;
    }

    public void writeTags (OSMEntity tagged) throws IOException {
        List<OSMEntity.Tag> tags = tagged.tags;
        // TODO This could stand a little more abstraction, like List<Tag> getTags()
        if (tagged.tags == null) {
            vout.writeUInt32(0);
        } else {
            vout.writeUInt32(tags.size());
            for (OSMEntity.Tag tag : tagged.tags) {
                if (tag.value == null) tag.value = "";
                vout.writeString(tag.key);
                vout.writeString(tag.value);
            }
        }
    }

    public boolean readBlock() throws IOException {
        // Reset delta coding fields
        lat = lon = id = ref = prevFixedLat = prevFixedLon = 0;
        int blockType = vin.readUInt32();
        if (blockType == VEX_NONE) return true; // NONE block indicates end of file
        boolean blockEnd = false;
        int nRead = 0;
        while ( ! blockEnd) {
            switch (blockType) {
                case VEX_NODE:
                    blockEnd = readNode();
                    break;
                case VEX_WAY:
                    blockEnd = readWay();
                    break;
                case VEX_RELATION:
                    blockEnd = readRelation();
                    break;
            }
            nRead += 1;
        }
        // We should have read the number of entities indicated by the input stream, plus the terminator entity.
        int expected_n_entities = vin.readUInt32();
        if (nRead != expected_n_entities + 1) {
            throw new IOException("Block length mismatch.");
        }
        return false;
    }

    public List<OSMEntity.Tag> readTags() throws IOException {
        OSMEntity tagged = new Node();
        int nTags = vin.readUInt32();
        for (int i = 0; i < nTags; i++) {
            String key = vin.readString();
            String val = vin.readString();
            tagged.addTag(key, val);
        }
        return tagged.tags;
    }


    public boolean readNode() throws IOException {
        /* Create a new instance each time because we don't know if this is going in a MapDB or a normal Map. */
        Node node = new Node();
        long idDelta = vin.readSInt64();
        if (idDelta == 0) return true;
        id += idDelta;
        node.tags = readTags();
        node.fixedLat = (int) (prevFixedLat + vin.readSInt64());
        node.fixedLon = (int) (prevFixedLon + vin.readSInt64());
        prevFixedLat = node.fixedLat;
        prevFixedLon = node.fixedLon;
        osm.nodes.put(id, node);
        return false;
    }

    public boolean readWay() throws IOException {
        /* Create a new instance each time because we don't know if this is going in a MapDB or a normal Map. */
        Way way = new Way();
        long idDelta = vin.readSInt64();
        if (idDelta == 0) return true;
        id += idDelta;
        way.tags = readTags();
        int nNodes = vin.readUInt32();
        way.nodes = new long[nNodes];
        for (int i = 0; i < nNodes; i++) {
            ref += vin.readSInt64();
            way.nodes[i] = ref;
        }
        osm.ways.put(id, way);
        return false;
    }

    /**
     * Output vex format from PBF without a round-trip through MapDB.\
     * This should wrap a VexFormatCodec reference rather than being an inner class of it.
     */
    private class Converter extends Parser {

        private int curr_etype;
        private int ecount;

        // This could be done in the constructor.
        private void streamBegin() throws IOException {
            vout = new VarIntOutputStream(new GZIPOutputStream(new FileOutputStream("/home/abyrd/test.vex")));
            vout.writeBytes(HEADER.getBytes());
            curr_etype = VEX_NONE;
            ecount = 0;
        }

        private void checkBlockTransition(int toEtype) throws IOException {
            if (curr_etype != toEtype) {
                if (curr_etype != VEX_NONE) {
                    endWriteBlock(ecount);
                    if (ecount < 1000) {
                        LOG.warn("Wrote very small block of length {}", ecount);
                    }
                    ecount = 0;
                }
                beginWriteBlock(toEtype);
                curr_etype = toEtype;
            }
        }

        @Override
        public void handleNode(long id, Node node) {
            try {
                checkBlockTransition(VEX_NODE);
                writeNode(id, node);
                ecount++;
            } catch (IOException ex) {
                throw new RuntimeException();
            }
        }

        @Override
        public void handleWay(long id, Way way) {
            try {
                checkBlockTransition(VEX_WAY);
                writeWay(id, way);
                ecount++;
            } catch (IOException ex) {
                throw new RuntimeException();
            }
        }

        @Override
        public void handleRelation(long id, Relation relation) {
            try {
                checkBlockTransition(VEX_RELATION);
                writeRelation(id, relation);
                ecount++;
            } catch (IOException ex) {
                throw new RuntimeException();
            }
        }
    }

    // TODO Need Etype for class, int for etype, etc.

    /** This main method will convert a PBF file to VEX in a streaming manner, without an intermediate datastore. */
    public static void main (String[] args) {
        // final String INPUT = "/var/otp/graphs/ny/new-york-latest.osm.pbf";
        final String INPUT = "/var/otp/graphs/nl/netherlands-latest.osm.pbf";
        // final String INPUT = "/var/otp/graphs/trimet/portland.osm.pbf";
        Converter converter = new VexFormatCodec().new Converter();
        try {
            converter.streamBegin();
            converter.parse(INPUT);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
