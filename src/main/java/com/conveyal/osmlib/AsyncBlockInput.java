package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Starts up a separate thread that reads full compressed data blocks one at a time into a memory buffer.
 * Whenever a block is fetched, it will trigger an asynchronous read-ahead and decompression of the next block.
 * TODO merge into deflated block reader, make it run itself.
 */
public class AsyncBlockInput {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockInput.class);

    private final DeflatedBlockReader blockReader;

    private final Thread blockReaderThread;

    /** Construct an asynchronous buffered inflater reading from the given input stream. */
    public AsyncBlockInput(InputStream upstream) {
        blockReader = new DeflatedBlockReader(upstream);
        blockReaderThread = new Thread(blockReader);
        blockReaderThread.start();
    }

    /**
     * @return the next block when it's available, the special END_BLOCK if there are no more blocks.
     */
    public VEXBlock nextBlock () {
        return blockReader.take();
    }

}


