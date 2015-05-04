package com.conveyal.osmlib;

import junit.framework.TestCase;

import java.io.*;
import java.util.Map;

public class RoundTripTest extends TestCase {

    static final String TEST_FILE = "./src/test/resources/bangor_maine.osm.pbf";
    //static final String TEST_FILE = "/home/abyrd/osm_data/tokyo_japan.osm.pbf";

    public void testVexFile() throws Exception {

        // Load OSM data from PBF
        OSM osmOriginal = new OSM(null);
        osmOriginal.loadFromPBFFile(TEST_FILE);
        assertTrue(osmOriginal.nodes.size() > 1);
        assertTrue(osmOriginal.ways.size() > 1);
        assertTrue(osmOriginal.relations.size() > 1);

        // Write OSM data out to a VEX file
        File vexFile = File.createTempFile("test", "vex");
        OutputStream outputStream = new FileOutputStream(vexFile);
        osmOriginal.writeVex(outputStream);

        // Read OSM data back in from VEX file
        OSM osmCopy = new OSM(null);
        InputStream inputStream = new FileInputStream(vexFile);
        osmCopy.readVex(inputStream);

        // Compare PBF data to VEX data
        compareOsm(osmOriginal, osmCopy);

    }

    public void testVexStream() throws Exception {

        // Create an input/output pipe pair so we can read in a VEX stream without using a file
        final PipedOutputStream outStream = new PipedOutputStream();
        final PipedInputStream inStream = new PipedInputStream(outStream);

        // Create a separate thread that will read a PBF file and convert it directly into a VEX stream
        new Thread(
            new Runnable() {
                public void run() {
                    try {
                        OSMEntitySink vexSink = new VexOutput(outStream);
                        FileInputStream pbfFileInputStream = new FileInputStream(TEST_FILE);
                        OSMEntitySource pbfSource = new PBFInput(pbfFileInputStream, vexSink);
                        pbfSource.read();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        ).start();

        // Stream VEX data in from the thread and put it in a MapDB
        OSM osmCopy = new OSM(null);
        osmCopy.readVex(inStream);

        // Load up the original PBF file for comparison
        OSM osmOriginal = new OSM(null);
        osmOriginal.loadFromPBFFile(TEST_FILE);

        // Compare PBF data to VEX stream
        compareOsm(osmOriginal, osmCopy);

    }

    private void compareOsm (OSM original, OSM copy) {
        compareMap(original.nodes, copy.nodes);
        compareMap(original.ways, copy.ways);
        compareMap(original.relations, copy.relations);
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