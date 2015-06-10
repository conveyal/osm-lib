package com.conveyal.osmlib.main;

import com.conveyal.osmlib.OSMEntitySink;
import com.conveyal.osmlib.OSMEntitySource;
import com.conveyal.osmlib.TextOutput;
import com.conveyal.osmlib.VexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class VEX2TXT {

    private static final Logger LOG = LoggerFactory.getLogger(VEX2TXT.class);

    /** This main method will stream a VEX file to text on stdout. */
    public static void main(String[] args) {

        // Get input and output file names
        if (args.length < 2) {
            System.err.println("usage: VEX2TXT input.pbf out.txt");
            System.exit(0);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Pump the entities from the VEX input directly to the text output.
        try {
            OSMEntitySink textSink = new TextOutput(new BufferedOutputStream(new FileOutputStream(outputPath)));
            OSMEntitySource vexSource = new VexInput(new FileInputStream(inputPath), textSink);
            vexSource.read();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

}
