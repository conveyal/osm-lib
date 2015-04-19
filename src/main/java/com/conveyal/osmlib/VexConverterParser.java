package com.conveyal.osmlib;

/**
 * A parser that converts PBF input directly to VEX output.
 * This stub cannot be easily converted without a refactor of the VexCodec.
 */
public class VexConverterParser extends Parser {

    int phase = VexFormatCodec.VEX_NODE;

    VexFormatCodec codec = new VexFormatCodec();

    public VexConverterParser(String osm) {
        super(null); // no MapDB OSM storage
    }

    @Override
    public void handleWay(long wayId, Way way) {
    };

}
