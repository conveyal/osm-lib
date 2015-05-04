package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Neither threadsafe nor reentrant! Create one new instance of the codec per encode operation.
 * Created by abyrd on 2015-05-04
 */
public class VexOutput implements OSMEntitySink {

    private static final Logger LOG = LoggerFactory.getLogger(VexOutput.class);

    private static final int MAX_BLOCK_ENTITIES = 64 * 1024;

    /* The underlying output stream where VEX data will be written. */
    private OutputStream outputStream;

    /* The underlying output stream where VEX data will be written. */
    private GZIPOutputStream gzipOutputStream;

    /* The output sink for uncompressed VEX format. */
    private VarIntOutputStream vout;

    /* Persistent values for delta decoding. */
    private long prevId, prevRef, prevFixedLat, prevFixedLon;

    /** Track the type of the current block to enforce grouping of entities. */
    private int blockEntityType = VexFormat.VEX_NONE;

    /** Track the number of entities that have been written out in the current block. */
    private int blockEntityCount = 0;

    /** Track the number of blocks that have been written out to the stream. */
    private int blockCount = 0;

    public VexOutput(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    private void writeBlockBegin(int eType) throws IOException {
        prevId = prevRef = prevFixedLat = prevFixedLon = 0;
        vout.writeUInt32(eType);
    }

    /** @param n - the number of entities that were written in this block. */
    private void writeBlockEnd(int n) throws IOException {
        vout.writeSInt64(0L);
        vout.writeUInt32(n);
    }

    /** Write the fields common to all OSM entities: ID and tags. Also increments the entity counter. */
    private void writeCommonFields(long id, OSMEntity osmEntity) throws IOException {
        vout.writeSInt64(id - prevId);
        prevId = id;
        writeTags(osmEntity);
        blockEntityCount += 1;
    }

    private void checkBlockTransition(int eType) throws IOException {
        /* If entity type changes (except at the beginning of the first block),
        write a block-end sentinel and an entity count. */
        if (blockEntityType != eType || blockEntityCount > MAX_BLOCK_ENTITIES) {
            if (blockEntityType != VexFormat.VEX_NONE) {
                writeBlockEnd(blockEntityCount);
                blockEntityCount = 0;
                blockCount += 1;
                LOG.info("Wrote block {}", blockCount);
            }
            writeBlockBegin(eType);
            blockEntityType = eType;
        }
    }

    private void writeTags(OSMEntity tagged) throws IOException {
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

    /* OSM DATA SINK INTERFACE */

    @Override
    public void writeBegin() throws IOException {
        LOG.info("Writing VEX format...");
        this.gzipOutputStream = new GZIPOutputStream(outputStream);
        this.vout = new VarIntOutputStream(gzipOutputStream);
        vout.writeBytes(VexFormat.HEADER);
    }

    @Override
    public void writeEnd() throws IOException {
        checkBlockTransition(VexFormat.VEX_NONE);
        writeBlockEnd(blockCount);
        gzipOutputStream.close(); // or .finish()
        LOG.info("Finished writing VEX format.");
    }

    @Override
    public void writeNode(long id, Node node) throws IOException {
        checkBlockTransition(VexFormat.VEX_NODE);
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

    @Override
    public void writeWay(long id, Way way) throws IOException {
        checkBlockTransition(VexFormat.VEX_WAY);
        writeCommonFields(id, way);
        vout.writeUInt32(way.nodes.length);
        for (long ref : way.nodes) {
            vout.writeSInt64(ref - prevRef);
            prevRef = ref;
        }
    }

    @Override
    public void writeRelation(long id, Relation relation) throws IOException {
        checkBlockTransition(VexFormat.VEX_RELATION);
        writeCommonFields(id, relation);
        vout.writeUInt32(relation.members.size());
        for (Relation.Member member : relation.members) {
            vout.writeSInt64(member.id);
            vout.writeUInt32(member.type.ordinal()); // FIXME bad, assign specific numbers
            vout.writeString(member.role);
        }
    }

}
