package com.conveyal.osmlib.main;

import com.conveyal.osmlib.OSMEntitySink;
import com.conveyal.osmlib.OSMEntitySource;
import com.conveyal.osmlib.PBFInput;
import com.conveyal.osmlib.TextOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class PBF2TXT {

    private static final Logger LOG = LoggerFactory.getLogger(PBF2TXT.class);

    public static void main(String[] args) {

        // Get input and output file names
        if (args.length < 2) {
            System.err.println("usage: PBF2TXT input.pbf out.txt");
            System.exit(0);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Pump the entities from the PBF input directly to the text output.
        try {
            OSMEntitySink textSink = new TextOutput(new BufferedOutputStream(new FileOutputStream(outputPath)));
            OSMEntitySource pbfSource = new PBFInput(new FileInputStream(inputPath), textSink);
            pbfSource.read();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

}
