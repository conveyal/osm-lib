package com.conveyal.osmlib;

public abstract class VexFormat {

    public static final byte[] HEADER = "VEXFMT".getBytes();
    public static final int VEX_NODE = 0;
    public static final int VEX_WAY = 1;
    public static final int VEX_RELATION = 2;
    public static final int VEX_NONE = 3;

    public static final int LAYER_ANY = 0;
    public static final int LAYER_STREET = 1;
    public static final int LAYER_LANDUSE = 2;
    public static final int LAYER_BUILDING = 3;

    // TODO Need Etype for class, int for etype, etc.

}
