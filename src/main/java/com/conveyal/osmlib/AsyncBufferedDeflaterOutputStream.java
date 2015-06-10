package com.conveyal.osmlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Accumulates data in a large in-memory buffer. When a write is about to cause the buffer to overflow, the contents
 * of the buffer are handed off to a secondary thread which writes the compressed data block to a downstream
 * OutputStream, preceded by the specified header bytes, the number of messages, and the number of compressed bytes.
 *
 * Double buffered, so it will block until the compression thread has finished writing before starting another one.
 * TODO extend OutputStream directly, and use explicit double buffers.
 * FIXME PROBLEM: we don't want entities to be split across blocks, so we can't trigger compression flush on a single-byte write.
 * solution: add a endMessage() method to OutputStream, buffer one message at a time.
 *
 * Java's ByteArrayOutputStream makes a copy when you fetch its backing byte array. Here, the compression process is
 * integrated directly into a subclass of ByteArrayOutputStream where the backing buffer is visible, avoiding this
 * copy step.
 */
public class AsyncBufferedDeflaterOutputStream extends MessageOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferedDeflaterOutputStream.class);

    /* At first I was using large buffers, but moving down from 32 to 16 to 8 to 2MB, final output keeps getting smaller! */
    public static final int BUFFER_SIZE = 1024 * 1024 * 2;

    public byte[] blockHeader = null;

    private byte[] bufA = new byte[BUFFER_SIZE];

    private byte[] bufB = new byte[BUFFER_SIZE];

    private byte[] buf = bufA;

    private int count = 0;

    private int countMessage = 0;

    private int nMessagesInBlock = 0;

    /* An active deflater thread. We should wait for it to finish its work before starting another one. */
    private Thread activeDeflaterThread = null;

    private OutputStream downstream;

    public AsyncBufferedDeflaterOutputStream (OutputStream downstream) {
        this.downstream = downstream;
    }

    /**
     * Note that this will cause an ArrayIndexOutOfBoundsException if you write more than MAX_MESSAGE_SIZE bytes
     * before calling endMessage().
     */
    @Override
    public void write (int b) {
        if (countMessage >= BUFFER_SIZE) {
            // Adding to the the current message fragment is going to overflow the buffer. Start a new block.
            endBlock();
        }
        buf[countMessage++] = (byte) b;
    }

    @Override
    public void endMessage() {
        nMessagesInBlock += 1;
        count = countMessage;
    }

    /**
     * Flush out any messages waiting in the buffer, ending the current block and starting a new one.
     * Note that this does _not_ include any waiting message fragment. You should call endMessage() first if you want
     * to include such a fragment.
     */
    public void endBlock() {
        // Wait for any running deflater thread to finish its work, we only have two buffers.
        if (activeDeflaterThread != null) {
            try {
                activeDeflaterThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (nMessagesInBlock > 0) {
            // Start a thread that writes the compressed block to the downstream OutputStream.
            Runnable runnable = new DeflatedBlockWriter(downstream, blockHeader, nMessagesInBlock, buf, count);
            activeDeflaterThread = new Thread(runnable);
            activeDeflaterThread.start();
            // Swap double buffers.
            final byte[] oldBuf = buf;
            buf = (buf == bufA) ? bufB : bufA;
            // Copy the message fragment from the old buffer to the beginning of the new buffer.
            int messageFragmentLength = countMessage - count;
            if (messageFragmentLength > 0) {
                System.arraycopy(oldBuf, count, buf, 0, messageFragmentLength);
            }
            countMessage = messageFragmentLength;
            count = 0;
            nMessagesInBlock = 0;
        }
    }

    /** Wait for any running deflater thread to finish its work, and write any buffered data out. */
    @Override
    public void flush() throws IOException {
        try {
            if (activeDeflaterThread != null) {
                activeDeflaterThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            downstream.flush();
        }
    }

}


