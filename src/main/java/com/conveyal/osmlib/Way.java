package com.conveyal.osmlib;

import java.io.Serializable;

public class Way extends OSMEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    public long[] nodes;
    
    @Override
    public String toString() {
        return String.format("Way with tags %s and nodes %s", tags, nodes);
    }
    
}