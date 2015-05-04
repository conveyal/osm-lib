package com.conveyal.osmlib;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface for classes that read in OSM entities from somewhere and pipe them into an OSMEntitySink
 * Created by abyrd on 2015-05-04
 */
public interface OSMEntitySource {

    /** Read the OSM entities from this source and pump them through to the sink. TODO read(sink) so OSM can implement this interface */
    public abstract void read() throws IOException;

}
