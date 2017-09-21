package com.conveyal.osmlib;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.conveyal.osmlib.TestUtils.dropDB;
import static com.conveyal.osmlib.TestUtils.generateNewDB;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Suite of tests to test the api of the VanillaExtract server
 */
public class VanillaExtractTest {
    static final String TEST_FILE = "./src/test/resources/porto_portugal.osm.pbf";

    private static String testDBName;

    /**
     * Drops the database made for this suite of tests
     */
    @AfterClass
    public static void tearDown() {
        dropDB(testDBName);
    }

    /**
     * Creates a new database, loads in some osm data into a Postgres Sink and then starts the Vanilla Extract server
     */
    @BeforeClass
    public static void setUp() throws Exception {
        testDBName = generateNewDB();
        if (testDBName == null) {
            throw new Exception("failed to setup test db");
        }

        String jdbcUrl = "jdbc:postgresql://localhost/" + testDBName;
        String[] postgresSinkArgs = {
            "./src/test/resources/bangor_maine.osm.pbf",
            jdbcUrl
        };

        // perform file to postgres load
        PostgresSink.main(postgresSinkArgs);
        VanillaExtract.startServer(jdbcUrl);
    }

    /**
     * Make sure the server is replying with a helpful message if an improper path is supplied.
     */
    @Test
    public void failsOnBadCoordinates() {
        String response = given().port(9002).get("/gimme-my-data.pdf").asString();

        // assert that response has expected error message
        assertThat(response, containsString("URI format"));
    }

    /**
     * Ensures that the server returns a valid extract of data.
     */
    @Test
    public void getsExtract() {
        String response = given().port(9002).get("/44.801884,-68.782802,44.805081,-68.779181.pbf").asString();

        // assert that the response is not empty
        assertThat(response.length(), greaterThan(0));

        // assert that the response is not an error response
        assertThat(response, not(containsString("URI format")));

        // assert that the response is a valid pbf with osm data
        OSM test = new OSM(null);
        test.readFromUrl("http://localhost:9002/44.801884,-68.782802,44.805081,-68.779181.pbf");
        assertThat(test.nodes.size(), equalTo(37));
        assertThat(test.relations.size(), equalTo(0));
        assertThat(test.ways.size(), equalTo(5));
    }
}
