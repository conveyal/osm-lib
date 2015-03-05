package com.conveyal.osmlib;

import junit.framework.TestCase;

public class OSMTest extends TestCase {
	public void testOSM(){
		OSM osm = new OSM(null);
		
		osm.fromPBF("./src/test/resources/bangor_maine.osm.pbf");
		
		assertTrue( osm.nodes.size() > 0 );
		
	}
}
