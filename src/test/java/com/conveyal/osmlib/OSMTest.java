package com.conveyal.osmlib;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;

import junit.framework.TestCase;

public class OSMTest extends TestCase {
	public void testOSM(){
		OSM osm = new OSM("./src/test/resources/tmp");
		osm.loadFromPBFFile("./src/test/resources/bangor_maine.osm.pbf");
		
		assertEquals( osm.nodes.size(), 35747 );
		assertEquals( osm.ways.size(), 2976 );
		assertEquals( osm.relations.size(), 34 );
		
	}
	
	public void tearDown() throws IOException{
		Files.delete( Paths.get("./src/test/resources/tmp") );
		Files.delete( Paths.get("./src/test/resources/tmp.p") );
	}
}
