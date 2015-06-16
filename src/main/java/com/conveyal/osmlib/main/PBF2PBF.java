package com.conveyal.osmlib.main;

import com.conveyal.osmlib.OSMEntitySink;
import com.conveyal.osmlib.OSMEntitySource;
import com.conveyal.osmlib.PBFInput;
import com.conveyal.osmlib.PBFOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class PBF2PBF {

    private static final Logger LOG = LoggerFactory.getLogger(PBF2PBF.class);

    /**
     * This main method will round-trip a PBF file through the osm-lib representation without using an intermediate MapDB.
     */
    public static void main(String[] args) {

        // Get input and output file names
        if (args.length < 2) {
            System.err.println("usage: PBF2PBF input.pbf output.pbf");
            System.exit(0);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Pump the entities from the PBF input directly to the PBF output.
        try {
            OSMEntitySink pbfSink = new PBFOutput(new FileOutputStream(outputPath));
            OSMEntitySource pbfSource = new PBFInput(new FileInputStream(inputPath), pbfSink);
            pbfSource.read();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

}
