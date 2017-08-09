package com.conveyal.osmlib;

import ch.qos.logback.core.db.ConnectionSource;
import ch.qos.logback.core.recovery.ResilientFileOutputStream;
import com.google.common.primitives.Longs;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.awt.font.FontRenderContext;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An OSM source that pulls OSM entities out of a Postgres database, within a geographic window.
 * NOT threadsafe or re-entrant. Make a new instance for each thread or nested operation.
 */
public class PostgresOSMSource implements OSMEntitySource {

    protected static final Logger LOG = LoggerFactory.getLogger(PostgresOSMSource.class);

    private double minLat, minLon, maxLat, maxLon;

    private String jdbcUrl = null;

    private DataSource dataSource;

    private Connection connection;

    private OSMEntitySink sink;

    // Avoid writing out shared/intersection nodes more than once.
    // We are currently doing this with an SQL "unique" constraint
    private NodeTracker nodesSeen = new NodeTracker();

    private TLongSet relationsSeen = new TLongHashSet();

    public PostgresOSMSource (String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public PostgresOSMSource (DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setBoundingBox(double minLat, double minLon, double maxLat, double maxLon) {
        if (minLat >= maxLat || minLon >= maxLon) {
            throw new IllegalArgumentException("Min must be smaller than max.");
        }
        this.minLat = minLat;
        this.minLon = minLon;
        this.maxLat = maxLat;
        this.maxLon = maxLon;
    }

    public void copyTo (OSMEntitySink sink) {
        this.sink = sink;
        try {
            if (dataSource == null) {
                connection = DriverManager.getConnection(jdbcUrl);
                connection.setReadOnly(true);
                connection.setAutoCommit(false);
            } else {
                connection = dataSource.getConnection();
            }
            sink.writeBegin();
            LOG.info("Reading nodes from Postgres...");
            processNodes();
            LOG.info("Reading ways from Postgres...");
            processWays();
            LOG.info("Reading relations from Postgres...");
            processRelations();
            sink.writeEnd();
            connection.close();
        } catch (Exception ex) {
            LOG.error("Exception occurred while copying from database.", ex);
        }
    }

    private void processNodes () throws Exception {
        // A primary key (unique index) on nodes makes version A reasonably fast.
        // If you have just a normal index, it is noticeably slower than extracting the nodes with a lat,lon index.
        // You can see the difference between the two, that the nodes extend outside the bounding box in version A.
        // Adding the distinct keyword here lets the DB server do the filtering, but may be problematic on larger extracts.
        final String sqlA = "select node_id, lat, lon, tags from" +
                "(select unnest(nodes) as node_id from ways " +
                "where rep_lat > ? and rep_lat < ? and rep_lon > ? and rep_lon < ?)" +
                "included_nodes join nodes using (node_id)";
        final String sqlB = "select node_id, lat, lon, tags from nodes where " +
                "lat > ? and lat < ? and lon > ? and lon < ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sqlA);
        preparedStatement.setDouble(1, minLat);
        preparedStatement.setDouble(2, maxLat);
        preparedStatement.setDouble(3, minLon);
        preparedStatement.setDouble(4, maxLon);
        // Configure the statement to stream results back with a cursor. TODO check that this actually helps.
        //preparedStatement.setFetchSize(500);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        LOG.info("Begin node iteration");
        while (resultSet.next()) {
            // Columns were specified in select, so we know their (one-based) sequence.
            long node_id = resultSet.getLong(1);
            // if (nodesSeen.contains(node_id)) continue;
            Node node = new Node();
            node.setLatLon(resultSet.getDouble(2), resultSet.getDouble(3));
            node.setTagsFromString(resultSet.getString(4));
            sink.writeNode(node_id, node);
            // nodesSeen.add(node_id);
        }
        LOG.info("End node iteration");
        resultSet.close();
        preparedStatement.close();
        nodesSeen = null; // free memory
    }

    private void processWays () throws Exception {
        final String sql = "select distinct way_id, tags, nodes " +
                "from ways " +
                "where rep_lat > ? and rep_lat < ? and rep_lon > ? and rep_lon < ?";
        final String sql2 = "select way_id, ways.tags, nodes from ways, nodes where nodes.node_id = ways.nodes[1] " +
                "and lat > ? and lat < ? and lon > ? and lon < ? ";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setDouble(1, minLat);
        preparedStatement.setDouble(2, maxLat);
        preparedStatement.setDouble(3, minLon);
        preparedStatement.setDouble(4, maxLon);
        //preparedStatement.setFetchSize(500);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        while (resultSet.next()) {
            Way way = new Way();
            // Columns were specified in select, so we know their (one-based) sequence.
            long way_id = resultSet.getLong(1);
            way.setTagsFromString(resultSet.getString(2));
            way.nodes = Arrays.stream((Long[])(resultSet.getArray(3).getArray())).mapToLong(Long::longValue).toArray();
            sink.writeWay(way_id, way);
        }
        resultSet.close();
        preparedStatement.close();
    }

    private void processRelations () throws Exception {
        // TODO Implement
    }

    /**
     * @param args JDBC_URL minLat minLon maxLat maxLon outputFile
     */
    public static void main (String[] args) {
        double minLat = Double.parseDouble(args[1]);
        double minLon = Double.parseDouble(args[2]);
        double maxLat = Double.parseDouble(args[3]);
        double maxLon = Double.parseDouble(args[4]);
        PostgresOSMSource source = new PostgresOSMSource(args[0]);
        source.setBoundingBox(minLat, minLon, maxLat, maxLon);
        OSMEntitySink sink = OSMEntitySink.forFile(args[5]);
        source.copyTo(sink);
    }

}
