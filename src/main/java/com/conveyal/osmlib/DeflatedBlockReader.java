package com.conveyal.osmlib;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Read in a deflated block.
 */
public class DeflatedBlockReader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferedDeflaterOutputStream.class);

    public static final int BUFFER_SIZE = 1024 * 1024 * 2;

    private final InputStream upstream;
    protected int entityType;
    protected int nEntities;
    protected byte[] inflatedData;
    protected int nBytes;
    protected boolean eof = false;

    public DeflatedBlockReader(InputStream upstream) {
        this.upstream = upstream;
    }

    /**
     */
    @Override
    public void run() {
        readHeader();
        if (eof) {
            return;
        }
        if (nBytes < 0 || nBytes > BUFFER_SIZE) {
            LOG.error("File contains strange compressed data size.");
            return;
        }
        try {
            byte[] deflatedData = new byte[nBytes];
            inflatedData = new byte[BUFFER_SIZE];
            ByteStreams.readFully(upstream, deflatedData);
            Inflater inflater = new Inflater();
            inflater.setInput(deflatedData);
            inflater.inflate(inflatedData);
            LOG.debug("Read block of {} bytes, inflated to {} bytes.", inflater.getTotalIn(), inflater.getTotalOut());
            LOG.debug("Expected to contain {} entities of type {}.", nEntities, entityType);
            inflater.end();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }

    private void readHeader() {
        byte[] fourBytes = new byte[4];
        try {
            ByteStreams.readFully(upstream, fourBytes);
            String s = new String(fourBytes);
            if (s.equals("VEXN")) {
                entityType = VexFormat.VEX_NODE;
            } else if (s.equals("VEXW")) {
                entityType = VexFormat.VEX_WAY;
            } else if (s.equals("VEXR")) {
                entityType = VexFormat.VEX_RELATION;
            } else {
                LOG.error("Unrecognized block type '{}', aborting VEX read.", entityType);
                throw new RuntimeException("Uncrecoginzed VEX block type.");
            }
            ByteStreams.readFully(upstream, fourBytes);
            nEntities = Ints.fromByteArray(fourBytes);
            ByteStreams.readFully(upstream, fourBytes);
            nBytes = Ints.fromByteArray(fourBytes);
        } catch (EOFException e) {
            // An EOF while reading the block header means there are no more blocks.
            LOG.debug("Hit end of file, no more blocks to read.");
            eof = true;
        } catch (IOException e) {
            // Other IO exceptions besides EOF are not expected.
            e.printStackTrace();
        }
    }
}
