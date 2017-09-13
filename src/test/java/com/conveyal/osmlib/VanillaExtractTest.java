package com.conveyal.osmlib;

import org.junit.BeforeClass;
import org.junit.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class VanillaExtractTest {
    static final String TEST_FILE = "./src/test/resources/porto_portugal.osm.pbf";

    @BeforeClass
    public static void setUp() {
        String[] args = {"jdbc:postgresql://localhost/osm_lib_test?user=osm_test&password=osm_test"};
        VanillaExtract.main(args);
    }

    @Test
    public void failsOnBadCoordinates() {
        String response = given().port(9002).get("/gimme-my-data.pdf").asString();

        // assert that response has expected error message
        assertThat(response, containsString("URI format"));
    }

    @Test
    public void getsExtract() {
        String response = given().port(9002).get("/44.801884,-68.782802,44.805081,-68.779181.pbf").asString();

        // assert that the response is not empty
        assertThat(response.length(), greaterThan(0));

        // assert that the response is not an error response
        assertThat(response, not(containsString("URI format")));

        // TODO: assert that the response is a valid pbf with osm data??
    }
}
