package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Reads full compressed data blocks one at a time into a memory buffer.
 * Whenever a block is fetched, it will trigger an asynchronous read-ahead and decompression of the next block.
 * Double buffered, so it will block until the compression thread has finished reading before handing off the block
 * ans starting another one.
 */
public class AsyncBufferedInflater {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferedInflater.class);

    /* An active inflater thread. We should wait for it to finish its work before starting another one. */

    private Thread activeInflaterThread = null;

    DeflatedBlockReader currBlock;

    DeflatedBlockReader nextBlock;

    private InputStream upstream;

    int nBlocksRead = 0;

    /** Construct an asynchronous buffered inflater reading from the given input stream. */
    public AsyncBufferedInflater(InputStream upstream) {
        this.upstream = upstream;
    }

    /**
     * Fetch the next block and make its contents available.
     * returns true if another block was made available, false if there are no more blocks.
     * Upon return, accessor methods will reflect the new block.
     * @return true if a new block was fetched, false if there are no more blocks in the file.
     */
    public boolean nextBlock () {
        if (activeInflaterThread == null) {
            // If this is the first block, there will be no active background inflater.
            nextBlock = new DeflatedBlockReader(upstream);
            activeInflaterThread = new Thread(nextBlock);
            activeInflaterThread.run(); // synchronous, no new thread
        } else {
            // Wait for any running inflater thread to finish its work, we only have two buffers.
            try {
                activeInflaterThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // Make new block available to the caller.
        currBlock = nextBlock;
        if (currBlock.eof) {
            return false; // End of file, there are no more blocks.
        }
        // Start another inflater thread for the next block.
        nextBlock = new DeflatedBlockReader(upstream);
        activeInflaterThread = new Thread(nextBlock);
        activeInflaterThread.start(); // start() is asynchronous, run in a new thread
        // Signal that a new block is available
        nBlocksRead += 1;
        return true;
    }

    public int getEntityType () {
        return currBlock.entityType;
    }

    public int getSizeBytes () {
        return currBlock.nBytes;
    }

    public int getEntityCount () {
        return currBlock.nEntities;
    }

    public byte[] getBytes () {
        return currBlock.inflatedData;
    }

}


