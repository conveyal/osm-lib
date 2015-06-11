package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.SynchronousQueue;

/**
 * A pipeline stage that reads in deflated VEX blocks.
 * Hands off the results of decompression synchronously through a zero-length blocking "queue".
 * This is meant to be run in a separate thread.
 */
public class DeflatedBlockReader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockOutputStream.class);

    private final SynchronousQueue<VEXBlock> synchronousQueue = new SynchronousQueue<>();

    private final InputStream upstream;

    public DeflatedBlockReader(InputStream upstream) {
        this.upstream = upstream;
    }

    /**
     * Wait for one block to be available, then return it. Returns null when there are no more blocks.
     */
    public VEXBlock take() {
        try {
            VEXBlock block = synchronousQueue.take();
            return block;
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for a block to become available. This shouldn't happen.");
            return VEXBlock.END_BLOCK;
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                VEXBlock block = new VEXBlock();
                block.readDeflated(upstream);
                if (block.entityType == VexFormat.VEX_NONE) {
                    // There are no more blocks, end of file.
                    synchronousQueue.put(VEXBlock.END_BLOCK);
                    break;
                } else {
                    synchronousQueue.put(block);
                }
            }
            upstream.close();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while trying to hand off a block. This shouldn't happen.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
