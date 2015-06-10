package com.conveyal.osmlib;

import java.io.OutputStream;

/**
 * An OutputStream that expects data to arrive in atomic chunks.
 * Implementations should avoid breaking up messages.
 */
public abstract class MessageOutputStream extends OutputStream {

    /** Call this method at the end of each message. */
    public abstract void endMessage();

}
