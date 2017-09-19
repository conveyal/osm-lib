package com.conveyal.osmlib;

import org.apache.commons.math3.random.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static String pgUrl = "jdbc:postgresql://localhost/postgres";

    /**
     * Forcefully drops a database even if other users are connected to it.
     *
     * @param dbName
     */
    public static void dropDB(String dbName) {
        // first, terminate all other user sessions
        executeAndClose("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
            "FROM pg_stat_activity " +
            "WHERE pg_stat_activity.datname = '" + dbName + "' " +
            "AND pid <> pg_backend_pid()");
        // drop the db
        executeAndClose("DROP DATABASE " + dbName);
    }

    /**
     * Boilerplate for opening a connection, executing a statement and closing connection.
     *
     * @param statement
     * @return true if everything worked.
     */
    private static boolean executeAndClose(String statement) {
        Connection connection;
        try {
            connection = DriverManager.getConnection(pgUrl);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error creating new database!");
            return false;
        }

        try {
            connection.prepareStatement(statement).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error creating new database!");
            return false;
        }

        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error closing connection!");
            return false;
        }
    }

    /**
     * Generate a new database for isolating a test.
     *
     * @return The name of the name database, or null if creation unsucessful
     */
    public static String generateNewDB() {
        String newDBName = randomIdString();
        if (executeAndClose("CREATE DATABASE " + newDBName)) {
            return newDBName;
        } else {
            return null;
        }
    }

    /**
     * Helper to return the relative path to a test resource file
     *
     * @param fileName
     * @return
     */
    public static String getResourceFileName(String fileName) {
        return "./src/test/resources/" + fileName;
    }

    /**
     * Generate a random unique prefix of n lowercase letters.
     * We can't count on sql table or schema names being case sensitive or tolerating (leading) digits.
     * For n=10, number of possibilities is 26^10 or 1.4E14.
     *
     * The approximate probability of a hash collision is k^2/2H where H is the number of possible hash values and
     * k is the number of items hashed.
     *
     * SHA1 is 160 bits, MD5 is 128 bits, and UUIDs are 128 bits with only 122 actually random.
     * To reach the uniqueness of a UUID you need math.log(2**122, 26) or about 26 letters.
     * An MD5 can be represented as 32 hex digits so we don't save much length, but we do make it entirely alphabetical.
     * log base 2 of 26 is about 4.7, so each character represents about 4.7 bits of randomness.
     *
     * The question remains of whether we need globally unique IDs or just application-unique IDs. The downside of
     * generating IDs sequentially or with little randomness is that when multiple databases are created we'll have
     * feeds with the same IDs as older or other databases, allowing possible accidental collisions with strange
     * failure modes.
     *
     *
     */
    public static String randomIdString() {
        MersenneTwister twister = new MersenneTwister();
        final int length = 27;
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) ('a' + twister.nextInt(26));
        }
        // Add a visual separator, which makes these easier to distinguish at a glance
        chars[4] = '_';
        return new String(chars);
    }
}

