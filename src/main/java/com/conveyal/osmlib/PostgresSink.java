package com.conveyal.osmlib;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.sql.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Load OSM into a simple SQL schema, but optimized for Postgres which has a text COPY command to initialize tables.
 *
 * Comparing Postgres to MapDB for Portland Oregon OSM:
 * MapDB 11 sec
 * Postgres 90 sec (batched prepared inserts, including building indexes)
 * Postgres 20 sec (using the COPY command)
 */
public class PostgresSink implements OSMEntitySink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

    private final String databaseUrl;
    private int nInserted = 0;

    // Keep track of which part of the input we're in. We only support the nodes, ways, relations order.
    private OSMEntity.Type phase = null;

    private PrintStream nodePrintStream = null;
    private PrintStream wayPrintStream = null;
    private PrintStream relationPrintStream = null;
    private PrintStream relationMemberPrintStream = null;

    public PostgresSink(String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }

    @Override
    public void writeBegin() throws IOException {
        try {
            Connection connection = DriverManager.getConnection(this.databaseUrl);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists updates");
            statement.execute("drop table if exists way_bins");
            statement.execute("drop table if exists nodes");
            statement.execute("drop table if exists ways");
            statement.execute("drop table if exists relations");
            statement.execute("drop table if exists relation_members");
            // We use column names node_id, way_id, relation_id instead of just id to facilitate joins.
            statement.execute("create table updates (time timestamp, replication_epoch bigint, operation varchar)");
            statement.execute("create table nodes (node_id bigint, lat float(9), lon float(9), tags varchar)");
            statement.execute("create table ways (way_id bigint, tags varchar, nodes bigint array)");
            statement.execute("create table relations (relation_id bigint, tags varchar)");
            statement.execute("create table relation_members (relation_id bigint, type varchar, member_id bigint, role varchar)");
            connection.commit();
            connection.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void setReplicationTimestamp(long secondsSinceEpoch) {
        // current_timestamp seems to be the only standard way to get the current time across all common databases.
    }

    /**
     * Start a Postgres COPY into the given table in another thread, returning a PrintStream into which the caller
     * should write the Postgres COPY text for that table.
     */
    private PrintStream initiatePostgresCopy (String tableName) {
        try {
            // Create an output stream that we will return to the caller.
            // The caller should then write Postgres COPY text to this output stream.
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            InputStream pipedInputStream = new PipedInputStream(pipedOutputStream, 1024 * 1024); // 1MB buffer
            new Thread(() -> {
                LOG.info("Loading database table '{}' via text COPY...", tableName);
                // CopyManager allows COPYing a stream over the network, and is only slightly slower than a COPY from local file.
                final String copySql = String.format("copy %s from stdin", tableName);
                Connection connection = null;
                try {
                    connection = DriverManager.getConnection(this.databaseUrl);
                    connection.setAutoCommit(false);
                    // Connection pools wrap the Connection objects. Unwrap the Postgres-specific connection interface.
                    CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
                    copyManager.copyIn(copySql, pipedInputStream, 1024 * 1024);
                } catch (Exception ex) {
                    throw new RuntimeException("Thread managing SQL COPY to Postgres database failed.", ex);
                } finally {
                    try {
                        if (connection != null) {
                            connection.commit();
                            connection.close();
                        }
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to commit after COPY into Postgres.", ex);
                    }
                    safeClose(pipedInputStream);
                }
                LOG.info("Finished Postgres COPY into table '{}', thread exiting.", tableName);
            }).start();
            // Replace any existing COPY output stream with the new one.
            return new PrintStream(pipedOutputStream, true, "UTF-8");
        } catch (Exception ex) {
            throw new RuntimeException("Unable to initiate SQL COPY to Postgres server.", ex);
        }
    }

    /**
     * Handle the fact that when we want to close streams in a finally clause, they may be null or closing them may
     * itself cause an exception.
     */
    private void safeClose (Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void writeNode(long id, Node node) throws IOException {
        if (phase == null) {
            // Transition to the nodes phase on encountering the first node.
            phase = OSMEntity.Type.NODE;
            nodePrintStream = initiatePostgresCopy("nodes");
            nInserted = 0;
        }
        if (phase != OSMEntity.Type.NODE) {
            throw new RuntimeException("Postgres module can only be loaded from sources following the standard nodes, ways, relations entity order.");
        }
        nodePrintStream.print(id);
        nodePrintStream.print('\t');
        nodePrintStream.print(node.getLat());
        nodePrintStream.print('\t');
        nodePrintStream.print(node.getLon());
        nodePrintStream.print('\t');
        nodePrintStream.print(clean(node.getTagsAsString()));
        nodePrintStream.print('\n');
        nInserted += 1;
        if (nInserted % 100000 == 0) LOG.info("Inserted {} nodes", human(nInserted));
    }

    @Override
    public void writeWay(long id, Way way) throws IOException {
        if (phase == OSMEntity.Type.NODE) {
            // Finalize any work on the previous segment of the input (nodes).
            safeClose(nodePrintStream);
            // Transition to the ways phase on encountering the first way after nodes.
            phase = OSMEntity.Type.WAY;
            wayPrintStream = initiatePostgresCopy("ways");
            nInserted = 0;
        }
        if (phase != OSMEntity.Type.WAY) {
            throw new RuntimeException("Postgres module can only be loaded from sources following the standard nodes, ways, relations entity order.");
        }
        // The way itself
        wayPrintStream.print(id);
        wayPrintStream.print('\t');
        wayPrintStream.print(clean(way.getTagsAsString()));
        wayPrintStream.print('\t');
        // We're storing the nodes in the way as a Postgres array type
        // Try: select id, unnest(nodes) from ways limit 100;
        wayPrintStream.print('{');
        wayPrintStream.print(Arrays.stream(way.nodes).mapToObj(Long::toString).collect(Collectors.joining(",")));
        wayPrintStream.print('}');
        wayPrintStream.print('\n');
        nInserted += 1;
        if (nInserted % 10000 == 0) LOG.info("Inserted {} ways", human(nInserted));
    }

    @Override
    public void writeRelation(long id, Relation relation) throws IOException {
        if (phase == OSMEntity.Type.WAY) {
            // Finalize any work on the previous segment of the input (ways).
            safeClose(wayPrintStream);
            // Transition to the relations phase on encountering the first relation after ways.
            phase = OSMEntity.Type.RELATION;
            relationPrintStream = initiatePostgresCopy("relations");
            relationMemberPrintStream = initiatePostgresCopy("relation_members");
            nInserted = 0;
        }
        if (phase != OSMEntity.Type.RELATION) {
            throw new RuntimeException("Postgres module can only be loaded from sources following the standard nodes, ways, relations entity order.");
        }
        // The relation itself
        relationPrintStream.print(id);
        relationPrintStream.print('\t');
        relationPrintStream.print(clean(relation.getTagsAsString()));
        relationPrintStream.print('\n');
        // The relation's members
        for (Relation.Member member : relation.members) {
            relationMemberPrintStream.print(id);
            relationMemberPrintStream.print('\t');
            relationMemberPrintStream.print(member.type);
            relationMemberPrintStream.print('\t');
            relationMemberPrintStream.print(member.id);
            relationMemberPrintStream.print('\t');
            relationMemberPrintStream.print(clean(member.role));
            relationMemberPrintStream.print('\n');
        }
        nInserted += 1;
        if (nInserted % 10000 == 0) LOG.info("Inserted {} relations", human(nInserted));
        // TODO IMPLEMENT RELATIONS
    }

    @Override
    public void writeEnd() throws IOException {
        // In case any transitions did not occur (a block of entities was missing) close all PrintStreams.
        safeClose(nodePrintStream);
        safeClose(wayPrintStream);
        safeClose(relationPrintStream);
        safeClose(relationMemberPrintStream);
        try {
            LOG.info("Indexing...");
            Connection connection = DriverManager.getConnection(this.databaseUrl);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.execute("create index nodes_id on nodes (node_id)");
//            statement.execute("create index nodes_lat on nodes (lat)");
//            statement.execute("create index nodes_lon on nodes (lon)");
            statement.execute("create index ways_id on ways (way_id)");
            statement.execute("create table way_bins as select way_id, floor(lat * 100) as y, floor(cos(radians(lat)) * lon * 100) as x from (select way_id, nodes[array_length(nodes, 1)/2] as node_id from ways) W join nodes using (node_id)");
            connection.commit();
            connection.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Example:
     * PostgresSink /Users/abyrd/r5/pdx/portland_oregon.osm.pbf  jdbc:postgresql://osm.blah.rds.amazonaws.com:5432/osm?user=USER&password=SECRET
     */
    public static void main (String[] args) {
        final String inputPath = args[0];
        final String databaseUrl = args[1];
        try {
            long startTime = System.currentTimeMillis();
            startTime = System.currentTimeMillis();
            OSMEntitySource source = OSMEntitySource.forFile(inputPath);
            OSMEntitySink sink = OSMEntitySink.forFile(databaseUrl);
            source.copyTo(sink);
            LOG.info("Total run time: {} sec", (System.currentTimeMillis() - startTime)/1000D);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    // We need to filter out backslashes from our strings, as well as delimiteres.
    // See https://www.postgresql.org/docs/9.6/static/sql-copy.html :
    // "Backslash characters (\) can be used in the COPY data to quote data characters that might otherwise be taken as
    // row or column delimiters. In particular, the following characters must be preceded by a backslash if they appear
    // as part of a column value: backslash itself, newline, carriage return, and the current delimiter character."
    // Note that in this regex, the four backslashes mean one literal backslash, i.e. (tab or CR or LF or backslash)
    final static Pattern pattern = Pattern.compile("\t|\n|\r|\\\\");

    /**
     * Remove tabs and linefeeds. They are allowed in tag values but will cause us grief so we're just
     * filtering them out of everything for now.
     * Horribly, a Java string can contain characters that can't actually be converted to UTF-8,
     * so we need to encode and decode the string to detect those, because they'll confuse Postgres.
     */
    private static String complicatedClean (String input) {
        String filtered = pattern.matcher(input).replaceAll(" ");
        if (!filtered.equals(input)) {
            LOG.warn("Stripped tabs and linefeeds out of string: " + input);
        }
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        CharBuffer charBuffer = CharBuffer.wrap(filtered);
        if (!encoder.canEncode(charBuffer)) {
            LOG.error("String contains something that can't be coded as UTF-8: " + filtered);
            try {
                encoder.onMalformedInput(CodingErrorAction.REPLACE);
                encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
                CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                filtered = String.valueOf(decoder.decode(encoder.encode(charBuffer)));
            } catch(CharacterCodingException e) {
                LOG.error("Failed round-trip through UTF-8: " + filtered);
                filtered = "BAD_STRING";
            }
        }
        if (!filtered.equals(input)) {
            LOG.info("Cleaned string is now: " + filtered);
        }
        return filtered;
    }

    private static String clean (String input) {
        String filtered = pattern.matcher(input).replaceAll(" ");
        if (!filtered.equals(input)) {
            LOG.warn("Stripped tabs, CR, LF, and/or backslash out of string " + filtered);
        }
        return filtered;
    }

    public static String human (int n) {
        if (n >= 1000000000) return String.format("%.1fG", n/1000000000.0);
        if (n >= 1000000) return String.format("%.1fM", n/1000000.0);
        if (n >= 1000) return String.format("%dk", n/1000);
        else return String.format("%d", n);
    }

}
