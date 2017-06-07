package com.conveyal.osmlib;

import com.conveyal.osmlib.main.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.stream.Collectors;

/**
 * Load OSM into a simple SQL schema.
 *
 * Comparing Postgres to MapDB:
 * MapDB 10.7 sec
 * Postgres 77.3 sec (batched prepared inserts, including building indexes)
 * Loading into Postgres using the COPY command could be faster but more complex.
 * MapDB does in fact work well as a temporary cache, we just don't want to persist it and re-open.
 */
public class SQLSink implements OSMEntitySink {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSink.class);

    String databaseUrl;
    Connection connection;
    private static int BATCH_SIZE = 5000;
    int nInserted = 0;

    PreparedStatement insertNodeStatement;
    PreparedStatement insertWayStatement;
    PreparedStatement insertWayNodeStatement;

    public SQLSink (String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }

    @Override
    public void writeBegin() throws IOException {
        try {
            connection = DriverManager.getConnection(this.databaseUrl);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists nodes");
            statement.execute("drop table if exists ways");
            statement.execute("drop table if exists way_nodes");
            statement.execute("create table nodes (id bigint, lat float(9), lon float(9), tags varchar)");
            statement.execute("create table ways (id bigint, tags varchar)");
            statement.execute("create table way_nodes (way_id bigint, seq integer, node_id bigint)");
            insertNodeStatement = connection.prepareStatement("insert into nodes values (?,?,?,?)");
            insertWayStatement = connection.prepareStatement("insert into ways values (?, ?)");
            insertWayNodeStatement = connection.prepareStatement("insert into way_nodes values (?,?,?)");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void setReplicationTimestamp(long secondsSinceEpoch) {

    }

    @Override
    public void writeNode(long id, Node node) throws IOException {
        try {
            insertNodeStatement.setLong(1, id);
            insertNodeStatement.setDouble(2, node.getLat());
            insertNodeStatement.setDouble(3, node.getLon());
            insertNodeStatement.setString(4, node.getTagsAsString());
            insertNodeStatement.addBatch();
            nInserted += 1;
            if (nInserted % BATCH_SIZE == 0) {
                insertNodeStatement.executeBatch();
                if (nInserted % 100000 == 0) LOG.info("Inserted {}", nInserted);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeWay(long id, Way way) throws IOException {
        try {
            insertWayStatement.setLong(1, id);
            insertWayStatement.setString(2, way.getTagsAsString());
            insertWayStatement.addBatch();
            nInserted += 1;
            int nodeSequence = 0;
            for (long node : way.nodes) {
                nodeSequence += 1;
                insertWayNodeStatement.setLong(1, id);
                insertWayNodeStatement.setInt(2, nodeSequence);
                insertWayNodeStatement.setLong(3, node);
                insertWayNodeStatement.addBatch();
            }
            if (nInserted % BATCH_SIZE == 0) {
                insertWayStatement.executeBatch();
                insertWayNodeStatement.executeBatch();
                if (nInserted % 10000 == 0) LOG.info("Inserted {}", nInserted);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeRelation(long id, Relation relation) throws IOException {
        // NO OP
    }

    @Override
    public void writeEnd() throws IOException {
        try {
            insertNodeStatement.executeBatch();
            insertWayStatement.executeBatch();
            insertWayNodeStatement.executeBatch();
            connection.commit();
            LOG.info("Indexing...");
            Statement statement = connection.createStatement();
            statement.execute("create index nodes_id on nodes (id)");
            statement.execute("create index nodes_lat on nodes (lat)");
            statement.execute("create index nodes_lon on nodes (lon)");
            statement.execute("create index ways_id on ways (id)");
            statement.execute("create index way_nodes_way_id on way_nodes (way_id)");
            statement.execute("create index way_nodes_node_id on way_nodes (node_id)");
            connection.commit();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main (String[] args) {
        final String inputPath = "/Users/abyrd/r5/pdx/portland_oregon.osm.pbf";
        final String SQLITE_FILE_URL = "jdbc:sqlite:/Users/abyrd/test-db";
        final String POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost/catalogue";
        try {
            long startTime = System.currentTimeMillis();
            OSMEntitySource source = OSMEntitySource.forFile(inputPath);
            OSMEntitySink sink = new OSM(null);
            source.copyTo(sink);
            LOG.info("Total run time: {} sec", (System.currentTimeMillis() - startTime)/1000D);
            //////////////////////
            startTime = System.currentTimeMillis();
            source = OSMEntitySource.forFile(inputPath);
            sink = new SQLSink(POSTGRES_LOCAL_URL);
            source.copyTo(sink);
            LOG.info("Total run time: {} sec", (System.currentTimeMillis() - startTime)/1000D);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
