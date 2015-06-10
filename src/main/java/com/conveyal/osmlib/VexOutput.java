package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Neither threadsafe nor reentrant! Create one new instance of the codec per encode operation.
 * Created by abyrd on 2015-05-04
 */
public class VexOutput implements OSMEntitySink {

    private static final Logger LOG = LoggerFactory.getLogger(VexOutput.class);

    /* The underlying output stream where VEX data will be written. */
    private OutputStream outputStream;

    /**/
    private AsyncBufferedDeflaterOutputStream deflaterOutputStream;

    /* The output sink for uncompressed VEX format. */
    private VarIntOutputStream vout;

    /* Persistent values for delta decoding. */
    private long prevId, prevRef, prevFixedLat, prevFixedLon;

    /** Track the type of the current block to enforce grouping of entities. */
    private int blockEntityType = VexFormat.VEX_NONE;

    public VexOutput(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    private void beginBlock(int eType) throws IOException {
        prevId = prevRef = prevFixedLat = prevFixedLon = 0;
        String hs;
        // TODO use Node.class instead of numeric constants
        switch (eType) {
            case VexFormat.VEX_NODE:
                hs = "VEXN";
                break;
            case VexFormat.VEX_WAY:
                hs = "VEXW";
                break;
            case VexFormat.VEX_RELATION:
                hs = "VEXR";
                break;
            default:
                hs = "NONE";
                break;
        }
        // Set the header that will be prepended to subsequent blocks when they are written out
        deflaterOutputStream.blockHeader = hs.getBytes();
    }

    /**
     * Force the current block to end, even when the output buffer is not full. Should be called when the
     * entity type changes.
     */
    private void endBlock() throws IOException {
        deflaterOutputStream.endBlock();
    }

    /**
     * Call at the beginning of each node, way, or relation.
     * Write the fields common to all OSM entities: ID and tags. Also increments the entity counter.
     */
    private void beginEntity(long id, OSMEntity osmEntity) throws IOException {
        long idDelta = id - prevId;
        if (idDelta == 0) {
            LOG.error("The same entity ID is being written twice in a row. This will prematurely terminate a block.");
        }
        vout.writeSInt64(idDelta);
        prevId = id;
        writeTags(osmEntity);
    }

    /** Call at the end of each node, way, or relation. */
    private void endEntity() {
        deflaterOutputStream.endMessage();
    }

    private void checkBlockTransition(int eType) throws IOException {
        /* If entity type changes (except at the beginning of the first block),
        write a block-end sentinel and an entity count. */
        if (blockEntityType != eType) {
            if (blockEntityType != VexFormat.VEX_NONE) {
                endBlock();
                String type = "entities";
                if (blockEntityType == VexFormat.VEX_NODE) type = "nodes";
                if (blockEntityType == VexFormat.VEX_WAY) type = "ways";
                if (blockEntityType == VexFormat.VEX_RELATION) type = "relations";
            }
            beginBlock(eType);
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
        deflaterOutputStream = new AsyncBufferedDeflaterOutputStream(outputStream);
        vout = new VarIntOutputStream(deflaterOutputStream);
    }

    @Override
    public void writeEnd() throws IOException {
        endBlock(); // Finish any partially-completed block.
        deflaterOutputStream.flush(); // Wait for async writes to complete and empty the buffer.
        vout.close(); // chain-close the DeflaterOutputStream and the downstream OutputStream.
        LOG.info("Finished writing VEX format.");
    }

    @Override
    public void writeNode(long id, Node node) throws IOException {
        checkBlockTransition(VexFormat.VEX_NODE);
        beginEntity(id, node);
        // plain ints should be fine rather than longs:
        // 2**31 = 2147483648
        // 180e7 = 1800000000.0
        long fixedLat = (long) (node.fixedLat);
        long fixedLon = (long) (node.fixedLon);
        vout.writeSInt64(fixedLat - prevFixedLat);
        vout.writeSInt64(fixedLon - prevFixedLon);
        prevFixedLat = fixedLat;
        prevFixedLon = fixedLon;
        endEntity();
    }

    @Override
    public void writeWay(long id, Way way) throws IOException {
        checkBlockTransition(VexFormat.VEX_WAY);
        beginEntity(id, way);
        vout.writeUInt32(way.nodes.length);
        for (long ref : way.nodes) {
            vout.writeSInt64(ref - prevRef);
            prevRef = ref;
        }
        endEntity();
    }

    @Override
    public void writeRelation(long id, Relation relation) throws IOException {
        checkBlockTransition(VexFormat.VEX_RELATION);
        beginEntity(id, relation);
        vout.writeUInt32(relation.members.size());
        for (Relation.Member member : relation.members) {
            vout.writeSInt64(member.id);
            vout.writeUInt32(member.type.ordinal()); // FIXME bad, assign specific numbers
            vout.writeString(member.role);
        }
        endEntity();
    }

}
