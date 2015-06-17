package com.conveyal.osmlib;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class OSMTest extends TestCase {
	public void testOSM(){
		OSM osm = new OSM("./src/test/resources/tmp");
		osm.readFromFile("./src/test/resources/bangor_maine.osm.pbf");
		assertEquals( osm.nodes.size(), 35747 );
		assertEquals( osm.ways.size(), 2976 );
		assertEquals( osm.relations.size(), 34 );
	}
	
	public void tearDown() throws IOException{
		Files.delete( Paths.get("./src/test/resources/tmp") );
		Files.delete( Paths.get("./src/test/resources/tmp.p") );
	}
}
