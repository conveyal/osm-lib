package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Neither threadsafe nor reentrant! Create one new instance of the codec per decode operation.
 * Created by abyrd on 2015-05-04
 */
public class VexInput implements OSMEntitySource {

    private static final Logger LOG = LoggerFactory.getLogger(VexInput.class);

    /* The input stream providing decompressed VEX format. */
    private VarIntInputStream vin;

    /* Persistent values for delta coding. */
    private long id, ref, prevFixedLat, prevFixedLon;

    private InputStream vexStream;

    private OSMEntitySink entitySink;

    public VexInput(InputStream vexStream, OSMEntitySink sink) {
        this.vexStream = vexStream;
        this.entitySink = sink;
    }

    @Override
    public void read() throws IOException {
        LOG.info("Reading VEX format...");
        this.vin = new VarIntInputStream(new GZIPInputStream(vexStream));
        byte[] header = vin.readBytes(VexFormat.HEADER.length);
        if ( ! Arrays.equals(header, VexFormat.HEADER)) {
            throw new IOException("Corrupt header.");
        }
        boolean done = false;
        long nBlocks = 0;
        while ( ! done) {
            done = readBlock();
            nBlocks += 1;
            LOG.info("Read block {}", nBlocks);
        }
        long sentinel = vin.readSInt64();
        long expectedBlocks = vin.readUInt64();
        if (sentinel != 0) {
            LOG.error("End block should not contain any elements.");
        }
        if (nBlocks != expectedBlocks + 1) {
            LOG.error("Did not read the expected number of blocks.");
        }
        LOG.info("Done reading.");
    }


    public boolean readBlock() throws IOException {
        // Reset delta coding fields
        id = ref = prevFixedLat = prevFixedLon = 0;
        int blockType = vin.readUInt32();
        if (blockType == VexFormat.VEX_NONE) return true; // NONE block indicates end of file
        boolean blockEnd = false;
        int nRead = 0;
        while ( ! blockEnd) {
            switch (blockType) {
                case VexFormat.VEX_NODE:
                    blockEnd = readNode();
                    break;
                case VexFormat.VEX_WAY:
                    blockEnd = readWay();
                    break;
                case VexFormat.VEX_RELATION:
                    blockEnd = readRelation();
                    break;
                default:
                    throw new RuntimeException("Unrecognized block type. Corrupt VEX data.");
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
        entitySink.writeNode(id, node);
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
        entitySink.writeWay(id, way);
        return false;
    }

    private static OSMEntity.Type[] memberTypeForOrdinal = OSMEntity.Type.values();

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
            member.type = memberTypeForOrdinal[vin.readUInt32()]; // FIXME bad, assign specific numbers
            member.role = vin.readString();
            relation.members.add(member);
        }
        entitySink.writeRelation(id, relation);
        //System.out.println(id + " " + relation.toString());
        return false;
    }

}
