package com.conveyal.osmlib.serializer;

import org.mapdb.Serializer;

import com.conveyal.osmlib.Way;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/** Ideally, these serializers would be the same ones used in the VEX binary exchange format. */
public class WaySerializer implements Serializer<Way>, Serializable {

	private static final long serialVersionUID = -6262949072197346414L;

	/** Delta-code the series of node references, and write out all values as varints. */
    public void serialize(DataOutput out, Way way) throws IOException {
        VarInt.writeRawVarint32(out, way.nodes.length);
        long lastNodeId = 0;
        for (int i = 0; i < way.nodes.length; i++) {
            long delta = way.nodes[i] - lastNodeId;
            VarInt.writeSInt64NoTag(out, delta);
            lastNodeId = way.nodes[i];
        }
        VarInt.writeTags(out, way);
    }

    public Way deserialize(DataInput in, int available) throws IOException {
        Way way = new Way();
        int nNodes = VarInt.readRawVarint32(in);
        way.nodes = new long[nNodes];
        long lastNodeId = 0;
        for (int i = 0; i < nNodes; i++) {
            lastNodeId += VarInt.readSInt64(in);
            way.nodes[i] = lastNodeId;
        }
        VarInt.readTags(in, way);
        return way;
    }

    public int fixedSize() { return -1; }

}