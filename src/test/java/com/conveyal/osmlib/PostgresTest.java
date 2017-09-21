package com.conveyal.osmlib;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests that verify osm data can be saved and queried from postgres
 */
public class PostgresTest {

    /**
     * Test that this library can load data into postgres and that the count of records matches expectations
     */
    @Test
    public void canLoadFromFileIntoDatabase() throws Exception {
        String newDBName = TestUtils.generateNewDB();
        if (newDBName == null) {
            throw new Exception("failed to generate test db");
        }
        try {
            String jdbcUrl = "jdbc:postgresql://localhost/" + newDBName;
            String[] args = {
                "./src/test/resources/bangor_maine.osm.pbf",
                jdbcUrl
            };

            // perform file to postgres load
            PostgresSink.main(args);

            // verify that data was loaded into postgres
            try {
                Connection connection = DriverManager.getConnection(jdbcUrl);
                TableTestCase[] tables = {
                    new TableTestCase("nodes", 35747),
                    new TableTestCase("relation_members", 435),
                    new TableTestCase("relations", 34),
                    new TableTestCase("updates", 0),
                    new TableTestCase("ways", 2976)
                };

                for (TableTestCase table : tables) {
                    PreparedStatement preparedStatement = connection.prepareStatement("Select count(*) from " + table.tableName);
                    preparedStatement.execute();
                    ResultSet resultSet = preparedStatement.getResultSet();
                    resultSet.next();
                    int numNodes = resultSet.getInt(1);
                    assertThat(numNodes, is(table.expectedNumNodes));
                }
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail();
            }
        } finally {
            TestUtils.dropDB(newDBName);
        }
    }
}

/**
 * Helper class to iterate through when testing whether the proper amount of records got loaded into a particular table.
 */
class TableTestCase {
    String tableName;
    int expectedNumNodes;

    public TableTestCase(String tableName, int expectedNumNodes) {
        this.tableName = tableName;
        this.expectedNumNodes = expectedNumNodes;
    }
}