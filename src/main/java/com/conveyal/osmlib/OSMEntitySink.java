package com.conveyal.osmlib;

import com.conveyal.osmlib.Node;
import com.conveyal.osmlib.Way;
import com.conveyal.osmlib.Relation;

import java.io.IOException;

/**
 * Created by abyrd on 2015-05-04
 *
 * TODO intersection finder, indexing sinks (or include those optionally in the OSM storage class itself.
 * TODO tag filter sink
 */
public interface OSMEntitySink {

    public void writeBegin() throws IOException;

    public void writeNode(long id, Node node) throws IOException;

    public void writeWay(long id, Way way) throws IOException;

    public void writeRelation(long id, Relation relation) throws IOException;

    public void writeEnd() throws IOException;

}
