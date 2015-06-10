package com.conveyal.osmlib;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;

/**
 *
 */
public class DeflatedBlockWriter implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferedDeflaterOutputStream.class);

    private final OutputStream downstream;
    private final byte[] header;
    private final int nMessages;
    private final byte[] dataBlock;
    private final int count;

    public DeflatedBlockWriter(OutputStream downstream, byte[] header, int nMessages, byte[] dataBlock, int count) {
        this.downstream = downstream;
        this.header = header;
        this.nMessages = nMessages;
        this.dataBlock = dataBlock;
        this.count = count;
    }

    /**
     * Deflates all bytes that have been accumulated in this in-memory buffer, then writes the deflated result to the
     * downstream OutputStream. This cannot be done incrementally or with a DeflaterOutputStream because
     * we need to write the compressed data length to the downstream OutputStream _before_ the compressed data.
     */
    @Override
    public void run() {
        LOG.debug("Compressing data block...");
        byte[] deflatedData = new byte[count];
        int deflatedSize = deflate(dataBlock, deflatedData, count);
        try {
            // Header, number of messages and size of compressed data as two 4-byte big-endian ints, compressed data.
            downstream.write(header);
            downstream.write(Ints.toByteArray(nMessages));
            downstream.write(Ints.toByteArray(deflatedSize));
            downstream.write(deflatedData, 0, deflatedSize);
            LOG.debug("Wrote block of {} bytes.", deflatedSize);
            LOG.debug("Contained {} entities with type {}.", nMessages, new String(header));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int deflate (byte[] input, byte[] output, int count) {
        int pos = 0;
        // Do not compress an empty data block, it will spin forever trying to fill the zero-length output buffer.
        if (count > 0) {
            Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, false); // include gzip header and checksum
            deflater.setInput(input, 0, count);
            deflater.finish(); // There will be no more input.
            while (!deflater.finished()) {
                pos += deflater.deflate(output, pos, output.length, Deflater.NO_FLUSH);
            }
        }
        return pos;
    }
}
