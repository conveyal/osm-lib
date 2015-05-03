package com.conveyal.osmlib;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

public class RoundTripTest extends TestCase {

    public void testVexFileCopyEqual() throws Exception {

        // Load OSM data from PBF
        OSM osmOriginal = new OSM(null);
        osmOriginal.loadFromPBFFile("./src/test/resources/bangor_maine.osm.pbf");
        //osmOriginal.loadFromPBFFile("./tokyo_japan.osm.pbf");
        assertTrue(osmOriginal.nodes.size() > 1);
        assertTrue(osmOriginal.ways.size() > 1);
        assertTrue(osmOriginal.relations.size() > 1);

        // Write OSM data out to a VEX file
        File vexFile = File.createTempFile("test", "vex");
        OutputStream outputStream = new FileOutputStream(vexFile);
        VexFormatCodec codec = new VexFormatCodec();
        codec.writeVex(osmOriginal, outputStream);

        // Read OSM data back in from VEX file
        OSM osmCopy = new OSM(null);
        codec = new VexFormatCodec();
        codec.readVex(new FileInputStream(vexFile), osmCopy);

        // Compare PBF data to VEX data
        compareMap(osmOriginal.nodes, osmCopy.nodes);
        compareMap(osmOriginal.ways, osmCopy.ways);
        compareMap(osmOriginal.relations, osmCopy.relations);

    }

    private <K,V> void compareMap (Map<K,V> m1, Map<K,V> m2) {
        assertEquals(m1.size(), m2.size());
        for (Map.Entry<K,V> entry : m1.entrySet()) {
            V e1 = entry.getValue();
            V e2 = m2.get(entry.getKey());
            assertEquals(e1, e2);
        }
    }

}