package com.conveyal.osmlib;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Load OSM data into MapDB and perform bounding box extracts.
 *
 * Some useful tools:
 * http://boundingbox.klokantech.com
 * http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
 */
public class VanillaExtract {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaExtract.class);

    private static final int PORT = 9002;

    private static final String BIND_ADDRESS = "0.0.0.0";

    private static final String USAGE = "";

    public static void main(String[] args) {

//        OSM osm = new OSM(args[0]);
//
//        if (args.length > 1 && args[1].startsWith("--load")) {
//            osm.intersectionDetection = true;
//            osm.tileIndexing = true;
//            if (args[1].equalsIgnoreCase("--loadurl")) {
//                osm.readFromUrl(args[2]);
//            } else {
//                osm.readFromFile(args[2]);
//            }
//            // TODO catch writing exceptions here and shut down properly, closing OSM database.
//            LOG.info("Done populating OSM database.");
//            osm.close();
//            return;
//        }
//
//        Thread updateThread = Updater.spawnUpdateThread(osm);

        DataSource dataSource = createDataSource(args[0], null, null);

        LOG.info("Starting VEX HTTP server on port {} of interface {}", PORT, BIND_ADDRESS);
        HttpServer httpServer = new HttpServer();
        httpServer.addListener(new NetworkListener("vanilla_extract", BIND_ADDRESS, PORT));
        // Bypass Jersey etc. and add a low-level Grizzly handler.
        // As in servlets, * is needed in base path to identify the "rest" of the path.
        httpServer.getServerConfiguration().addHttpHandler(new VexHttpHandler(dataSource), "/*");
        try {
            httpServer.start();
            LOG.info("VEX server running.");
            Thread.currentThread().join();
//            updateThread.interrupt();
        } catch (BindException be) {
            LOG.error("Cannot bind to port {}. Is it already in use?", PORT);
        } catch (IOException ioe) {
            LOG.error("IO exception while starting server.");
        } catch (InterruptedException ie) {
            LOG.info("Interrupted, shutting down.");
        }
        httpServer.shutdown();
    }

    // Planet files are named planet-150504.osm.pbf (YYMMDD format)
    // TODO auto-choose a DL mirror using ping?
    public long timestampForPlanetFile(String filename) {
        Pattern longDatePattern = Pattern.compile("\\d{8}"); // look for eight digits
        Pattern shortDatePattern = Pattern.compile("\\d{6}"); // look for six digits
        Matcher dateMatcher = longDatePattern.matcher(filename);
        if (dateMatcher.matches()) {
            // TODO
        } else {
            dateMatcher = longDatePattern.matcher(filename);
            if (dateMatcher.matches()) {
                // TODO
            }
        }
        return 0;
    }

    private static class VexHttpHandler extends HttpHandler {

        private static DataSource dataSource;

        public VexHttpHandler(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public void service(Request request, Response response) throws Exception {
            response.setContentType("application/osm");
            String uri = request.getDecodedRequestURI();
            int suffixIndex = uri.lastIndexOf('.');
            String fileType = uri.substring(suffixIndex);
            OutputStream outStream = response.getOutputStream();
            try {
                String[] coords = uri.substring(1, suffixIndex).split("[,;]");
                double minLat = Double.parseDouble(coords[0]);
                double minLon = Double.parseDouble(coords[1]);
                double maxLat = Double.parseDouble(coords[2]);
                double maxLon = Double.parseDouble(coords[3]);
                if (minLat >= maxLat || minLon >= maxLon || minLat < -90 || maxLat > 90 || minLon < -180 || maxLon > 180) {
                    throw new IllegalArgumentException();
                }
                /* Respond to head requests to let the client know the server is alive and the request is valid. */
                if (request.getMethod() == Method.HEAD) {
                    response.setStatus(HttpStatus.OK_200);
                    return;
                }
                /* TODO filter out buildings on the server side. */
                boolean buildings = coords.length > 4 && "buildings".equalsIgnoreCase(coords[4]);

                OSMEntitySink sink = OSMEntitySink.forStream(uri, outStream);
                PostgresOSMSource osmSource = new PostgresOSMSource(dataSource);
                osmSource.setBoundingBox(minLat, minLon, maxLat, maxLon);
                osmSource.copyTo(sink);
                response.setStatus(HttpStatus.OK_200);
            } catch (Exception ex) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                outStream.write("URI format: /min_lat,min_lon,max_lat,max_lon[.pbf|.vex] (all coords in decimal degrees)\n".getBytes());
                ex.printStackTrace();
            } finally {
                outStream.close();
            }
        }

    }

    public static DataSource createDataSource(String url, String username, String password) {
        // ConnectionFactory can handle null username and password (for local host-based authentication)
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, username, password);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);
        // TODO: set other options on connectionPool?
        connectionPool.setMaxTotal(300);
        connectionPool.setMaxIdle(4);
        connectionPool.setMinIdle(2);
        poolableConnectionFactory.setPool(connectionPool);
        // We also want auto-commit switched off for bulk inserts, and also because fetches are super-slow with
        // auto-commit turned on. Apparently it interferes with result cursors.
        poolableConnectionFactory.setDefaultAutoCommit(false);
        return new PoolingDataSource(connectionPool);
    }

}