package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * Accumulates data in a large in-memory buffer. When a write is about to cause the buffer to overflow, the contents
 * of the buffer are handed off to a secondary thread which writes the compressed data block to a downstream
 * OutputStream, preceded by the specified header bytes, the number of messages, and the number of compressed bytes.
 *
 * Java's ByteArrayOutputStream makes a copy when you fetch its backing byte array. Here, the output buffer and
 * compression process are integrated directly, avoiding this copy step.
 */
public class AsyncBlockOutputStream extends MessageOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockOutputStream.class);

    /**
     * The maximum expected message size. A block will be considered finished when it is within this distance
     * of the maximum block size.
     */
    public static final int MAX_MESSAGE_SIZE = 1024 * 16;

    private int currEntityType;

    private byte[] buf = new byte[VEXBlock.BUFFER_SIZE];

    private int pos = 0;

    private int nMessagesInBlock = 0;

    private final DeflatedBlockWriter blockWriter;

    private final Thread blockWriterThread;

    public AsyncBlockOutputStream(OutputStream downstream) {
        blockWriter = new DeflatedBlockWriter(downstream);
        blockWriterThread = new Thread(blockWriter);
        blockWriterThread.start();
    }

    @Override
    public void close() {
        try {
            // This will let the writing finish, then break out of the polling loop.
            blockWriter.handOff(VEXBlock.END_BLOCK);
            blockWriterThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Affects the header that will be prepended to subsequent blocks when they are written out. */
    public void setEntityType(int entityType) {
        this.currEntityType = entityType;
    }

    /** Add a byte to the message fragment currently being constructed, flushing out a block as needed. */
    @Override
    public void write (int b) {
        // TODO catch out of range exceptions and warn that the message was too big.
        buf[pos++] = (byte) b;
    }

    /**
     * Declare the message fragment under construction to be complete.
     * When there's not much space left in the buffer, this will end the block and start a new one.
     */
    @Override
    public void endMessage() {
        nMessagesInBlock += 1;
        if (pos > buf.length - MAX_MESSAGE_SIZE) {
            endBlock();
        }
    }

    /**
     * Flush out any messages waiting in the buffer, ending the current block and starting a new one.
     * Note that this does _not_ include any waiting message fragment. You should call endMessage() first if you want
     * to include such a fragment.
     */
    public void endBlock() {
        if (nMessagesInBlock > 0) {

            // Make a VEX block object to pass off to the compression/writer thread
            VEXBlock block = new VEXBlock();
            block.data = buf;
            block.nBytes = pos;
            block.entityType = currEntityType;
            block.nEntities = nMessagesInBlock;

            // Send this block to the compression/writer thread synchronously (call blocks until thread is ready)
            blockWriter.handOff(block);

            // Create a new buffer and reset the position and message counters
            buf = new byte[VEXBlock.BUFFER_SIZE];
            pos = 0;
            nMessagesInBlock = 0;

        }
    }

}


