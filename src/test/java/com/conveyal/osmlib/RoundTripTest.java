package com.conveyal.osmlib;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class RoundTripTest extends TestCase {

    public void testVexFileCopyEqual() throws Exception {

        // Load OSM data from PBF
        OSM osmOriginal = new OSM(null);
        osmOriginal.loadFromPBFFile("/var/otp/graphs/portland/portland_oregon.osm.pbf");
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

        // Compare PBF data to VEX data using Map.equals() which checks whether two maps have the same entrySet.
        assertEquals(osmOriginal.nodes, osmCopy.nodes);
        assertEquals(osmOriginal.ways, osmCopy.ways);
        assertEquals(osmOriginal.relations, osmCopy.relations);

    }

}