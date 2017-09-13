package com.conveyal.osmlib;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PostgresTest {

    @Test
    public void canLoadFromFileIntoDatabase() {
        String jdbcUrl = "jdbc:postgresql://localhost/osm_lib_test?user=osm_test&password=osm_test";
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

            for (TableTestCase table: tables) {
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
    }
}

class TableTestCase {
    String tableName;
    int expectedNumNodes;

    public TableTestCase(String tableName, int expectedNumNodes) {
        this.tableName = tableName;
        this.expectedNumNodes = expectedNumNodes;
    }
}