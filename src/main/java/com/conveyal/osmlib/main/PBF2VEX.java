package com.conveyal.osmlib.main;

import com.conveyal.osmlib.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

// Compression is much slower than decompression. Disk access is also a major factor in slowness for large extracts.
public class PBF2VEX {

    private static final Logger LOG = LoggerFactory.getLogger(PBF2VEX.class);

    /** This main method will convert a PBF file to VEX without using an intermediate MapDB datastore. */
    public static void main(String[] args) {

        // Get input and output file names
        if (args.length < 2) {
            System.err.println("usage: PBF2VEX input.pbf output.vex");
            System.exit(0);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Pump the entities from the PBF input directly to the VEX output.
        try {
            OSMEntitySink vexSink = new VexOutput(new FileOutputStream(outputPath));
            OSMEntitySource pbfSource = new PBFInput(new FileInputStream(inputPath), vexSink);
            pbfSource.read();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    public static void analyzeTags(OSM osm) {
        Multimap<String, String> kv = HashMultimap.create();
        for (Way way : osm.ways.values()) {
            for (OSMEntity.Tag tag : way.tags) {
                kv.put(tag.key, tag.value);
            }
        }
        for (String k : kv.keySet()) {
            LOG.info("{} = {}", k, kv.get(k));
        }
    }

}
