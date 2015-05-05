package com.conveyal.osmlib;

import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.util.SortedSet;
import java.util.zip.GZIPOutputStream;

/**
 * Load OSM data into MapDB and perform bounding box extracts.
 *
 * Some useful tools:
 * http://boundingbox.klokantech.com
 * http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
 */
public class VanillaExtract {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaExtract.class);

    private static final int PORT = 9001;

    private static final String BIND_ADDRESS = "0.0.0.0";

    /** VanillaExtract /path/to/storageFile inputFile.pbf */
    public static void main(String[] args) {

        OSM osm = new OSM(args[0]);
        osm.intersectionDetection = true;
        osm.tileIndexing = true;
        osm.loadFromPBFFile(args[1]);

        LOG.info("Starting VEX HTTP server on port {} of interface {}", PORT, BIND_ADDRESS);
        HttpServer httpServer = new HttpServer();
        httpServer.addListener(new NetworkListener("vanilla_extract", BIND_ADDRESS, PORT));
        // Bypass Jersey etc. and add a low-level Grizzly handler.
        // As in servlets, * is needed in base path to identify the "rest" of the path.
        httpServer.getServerConfiguration().addHttpHandler(new VexHttpHandler(osm), "/*");
        try {
            httpServer.start();
            LOG.info("VEX server running.");
            Thread.currentThread().join();
        } catch (BindException be) {
            LOG.error("Cannot bind to port {}. Is it already in use?", PORT);
        } catch (IOException ioe) {
            LOG.error("IO exception while starting server.");
        } catch (InterruptedException ie) {
            LOG.info("Interrupted, shutting down.");
        }
        httpServer.shutdown();

    }

    private static class VexHttpHandler extends HttpHandler {

        private static OSM osm;

        public VexHttpHandler(OSM osm) {
            this.osm = osm;
        }

        @Override
        public void service(Request request, Response response) throws Exception {

            String uri = request.getDecodedRequestURI();
            response.setContentType("application/gzip");
            OutputStream outStream = response.getOutputStream();
            try {
                String[] coords = uri.split("/")[1].split("[,;]");
                double minLat = Double.parseDouble(coords[0]);
                double minLon = Double.parseDouble(coords[1]);
                double maxLat = Double.parseDouble(coords[2]);
                double maxLon = Double.parseDouble(coords[3]);
                if (minLat >= maxLat || minLon >= maxLon) {
                    throw new IllegalArgumentException();
                }
                /* Respond to head requests to let the client know the server is alive and the request is valid. */
                if (request.getMethod() == Method.HEAD) {
                    response.setStatus(HttpStatus.OK_200);
                    return;
                }
                /* TODO filter out buildings on the server side. */
                boolean buildings = coords.length > 4 && "buildings".equalsIgnoreCase(coords[4]);

                OSMEntitySink vexOutput = new VexOutput(outStream);
                TileOSMSource tileSource = new TileOSMSource(osm);
                tileSource.setBoundingBox(minLat, minLon, maxLat, maxLon);
                tileSource.sink = vexOutput;
                tileSource.read();
                response.setStatus(HttpStatus.OK_200);
            } catch (Exception ex) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                outStream.write("URI format: /min_lat,min_lon,max_lat,max_lon (all in decimal degrees)\n".getBytes());
                ex.printStackTrace();
            } finally {
                outStream.close();
            }
        }

    }
}
