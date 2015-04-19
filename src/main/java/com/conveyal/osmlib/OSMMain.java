package com.conveyal.osmlib;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A proof of concept for loading OSM data from PBF into MapDB, then finding intersections and generating edges from
 * the disk-backed data. The intent is to allow working with very large OSM files without consuming too much memory
 * during the graph build process -- it is part of the same set of experiments that loads GTFS into MapDB.
 *
 * Loading the ways into a MapDB may not even be necessary. It should be possible to generate the edges on the fly, one
 * way at a time. The same is true of the nodes: we could just scan through them and only track which are within the
 * bounding geometry, then which are intersections, then immediately create vertices rather than ever saving the node
 * objects (as long as these intersection vertices are indexed by OSM ID).
 *
 * Some observations:
 * The Netherlands PBF is 900MB, and a MapDB using Treemaps is 2.1GB.
 * Loading the PBF into MapDB takes 6 minutes for nodes, 1 minute for ways, and 35 seconds to find intersections.
 * However, this is loading
 * During tests, the JVM ran out of memory when making edges because currently we keeps all edges in memory.
 * Note that these maps contain only routable ways, not all ways (see org.opentripplanner.osm.Parser#retainKeys).
 *
 * A "compressed" DB is about 1/3 smaller than an uncompressed one.
 * The "compact" operation only reduces an uncompressed DB's size by about 1/6.
 * The "compact" operation does not reduce the size of the compressed DB at all.
 *
 * @author abyrd
 */
public class OSMMain {

    private static final Logger LOG = LoggerFactory.getLogger(OSMMain.class);
    static final Envelope ENV = new Envelope(4.4, 5.5, 52.2, 53.3);

    /** This main method will convert a PBF file to VEX using an intermediate MapDB datastore. */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("usage: OSMMain input.pbf [output.vex] [/path/to/storage.file]");
            System.exit(0);
        }
        String inputLocation = args[0];
        String outputLocation = "out.vex";
        if (args.length > 1) {
            outputLocation = args[1];
        }
        String storageLocation = null;
        if (args.length > 2) {
            storageLocation = args[2];
        }
        OSM osm = new OSM(storageLocation); // null will use a temp file
        osm.loadFromPBFFile(inputLocation);
        try (OutputStream fout = new FileOutputStream(outputLocation)) {
            LOG.info("writing vex to '{}'...", outputLocation);
            new VexFormatCodec().writeVex(osm, fout);
            LOG.info("done writing vex.");
        } catch (FileNotFoundException ex) {
            LOG.error("File not found exception");
        } catch (IOException ex) {
            LOG.error("IO exception");
        }
    }

    public static void analyzeTags(OSM osm) {
        Multimap<String, String> kv = HashMultimap.create();
        for (Way way : osm.ways.values()) {
            for (OSMEntity.Tag tag : way.tags) {
                kv.put(tag.key, tag.value);
            }
        }
        for (String k : kv.keySet()) {
            LOG.info("{} = {}", k, kv.get(k));
        }
    }

}
