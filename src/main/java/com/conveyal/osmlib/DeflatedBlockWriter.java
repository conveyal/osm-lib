package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.SynchronousQueue;

/**
 * A pipeline stage that receives uncompressed VEX blocks and writes them out in compressed form.
 * Receives blocks for compression synchronously through a zero-length blocking "queue".
 * This is meant to be run in a separate thread.
 */
public class DeflatedBlockWriter implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockOutputStream.class);

    private final SynchronousQueue<VEXBlock> synchronousQueue = new SynchronousQueue<>();

    private final OutputStream downstream;

    /** Create a DeflatedBlockWriter that writes deflated data to the given OutputStream. */
    public DeflatedBlockWriter(OutputStream downstream) {
        this.downstream = downstream;
    }

    /**
     * Hand off a block for writing. Handing off a block with element type NONE signals the end of ourput, and
     * will shut down the writer thread.
     */
    public void handOff(VEXBlock vexBlock) {
        try {
            synchronousQueue.put(vexBlock);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes VEXBlocks one by one from the synchronous blocking queue.
     * Deflates all the bytes that have been accumulated in each block and writes the deflated result to the
     * downstream OutputStream. The compression cannot be done incrementally or with a DeflaterOutputStream because
     * we need to write the compressed data length to the downstream OutputStream _before_ the compressed data.
     */
    @Override
    public void run() {

        while (true) {
            try {
                VEXBlock block = synchronousQueue.take(); // block until work is available
                if (block == VEXBlock.END_BLOCK) break;
                block.writeDeflated(downstream);
            } catch (InterruptedException ex) {
                // Preferably, we'd like to use a thread interrupt to tell the thread to shut down when there's no more
                // input. It should finish writing the last block before exiting.
                // InterruptedException should only happen during interruptable activity like sleeping or polling,
                // and we don't expect it to happen during I/O: http://stackoverflow.com/a/10962613/778449
                // However when writing to a PipedOutputStream, blocked write() calls can also notice the interrupt and
                // abort with an InterruptedIOException so this is not viable. Instead we use a special sentinel block.
                LOG.error("Block writer thread was interrupted while waiting for work.");
                break;
            }
        }
        // For predictability only one thread should write to a stream, and that thread should close the stream.
        // Or at least this is what piped streams impose.
        // See https://techtavern.wordpress.com/2008/07/16/whats-this-ioexception-write-end-dead/
        try {
            downstream.flush();
            downstream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


}
