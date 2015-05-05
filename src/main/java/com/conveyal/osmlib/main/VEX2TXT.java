package com.conveyal.osmlib.main;

import com.conveyal.osmlib.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

public class VEX2TXT {

    private static final Logger LOG = LoggerFactory.getLogger(VEX2TXT.class);

    /** This main method will stream a VEX file to text on stdout. */
    public static void main(String[] args) {
        // Get input and output file names
        if (args.length < 1) {
            System.err.println("usage: VEX2TXT input.pbf");
            System.exit(0);
        }
        String inputPath = args[0];
        // Pump the entities from the PBF input directly to the VEX output.
        try {
            OSMEntitySink textSink = new TextOutput(System.out);
            OSMEntitySource vexSource = new VexInput(new FileInputStream(inputPath), textSink);
            vexSource.read();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
