package com.conveyal.osmlib;

import junit.framework.TestCase;
import org.mapdb.Fun;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.LongStream;

public class OSMTest extends TestCase {
	private OSM osm;

	public void setUp() throws Exception {
		osm = new OSM("./src/test/resources/tmp");

	}

	public void testOSM(){
		osm.readFromFile("./src/test/resources/bangor_maine.osm.pbf");
		assertEquals( osm.nodes.size(), 35747 );
		assertEquals( osm.ways.size(), 2976 );
		assertEquals( osm.relations.size(), 34 );

		// make sure the indices work
		for (Map.Entry<Long, Relation> e : osm.relations.entrySet()) {
			Relation relation = e.getValue();
			long id = e.getKey();
			// Tested: Bangor contains relations with way, node, and relation members
			for (Relation.Member member : relation.members) {
				if (member.type == OSMEntity.Type.NODE)
					assertTrue(osm.relationsByNode.contains(Fun.t2(member.id, id)));
				else if (member.type == OSMEntity.Type.WAY)
					assertTrue(osm.relationsByWay.contains(Fun.t2(member.id, id)));
				else if (member.type == OSMEntity.Type.RELATION)
					assertTrue(osm.relationsByRelation.contains(Fun.t2(member.id, id)));
			}
		}
	}

	public void testIntersectionNodes() throws Exception {
		Way way1 = new Way();
		way1.nodes = new long[] { 1, 55, 22, 13 };
		osm.findIntersectionNodes(way1);

		//No intersections currently
		assertTrue(
			LongStream.of(way1.nodes).allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));

		//This way intersects with previous one in node 55
		Way wayIntersectsWithWay1 = new Way();
		wayIntersectsWithWay1.nodes = new long[] { 5, 88, 3, 18, 55 };
		osm.findIntersectionNodes(wayIntersectsWithWay1);

		long[] nonIntersections = new long[] { 5, 88, 3, 18 };

		assertTrue(LongStream.of(nonIntersections)
			.allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));
		assertTrue(osm.intersectionNodes.contains(55));

		//Node with duplicate nodes which shouldn't be counted as intersection
		Way wayWithDuplicateNodes = new Way();
		wayWithDuplicateNodes.nodes = new long[] { 8, 7, 7, 33, 15 };
		nonIntersections = new long[] { 8, 7, 33, 15 };

		osm.findIntersectionNodes(wayWithDuplicateNodes);

		assertTrue(LongStream.of(nonIntersections)
			.allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));

		//Node with duplicate nodes which are actually an intersection
		Way wayWithDuplicateNodesAndIntersection = new Way();
		wayWithDuplicateNodesAndIntersection.nodes = new long[] { 2, 98, 3, 3 };
		nonIntersections = new long[] { 2, 98 };
		osm.findIntersectionNodes(wayWithDuplicateNodesAndIntersection);

		assertTrue(LongStream.of(nonIntersections)
			.allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));
		assertTrue(osm.intersectionNodes.contains(3));

		//Way with duplicate non consecutive nodes are actually an self intersection (node 34 self intersects)
		Way wayWithDuplicateNonConsecutiveNodesAndIntersection = new Way();
		wayWithDuplicateNonConsecutiveNodesAndIntersection.nodes = new long[] { 50, 34, 105, 111, 34, 85 };
		nonIntersections = new long[] { 50, 105, 111, 85 };
		osm.findIntersectionNodes(wayWithDuplicateNonConsecutiveNodesAndIntersection);

		assertTrue(LongStream.of(nonIntersections)
			.allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));
		assertTrue(osm.intersectionNodes.contains(34));

		//Way with duplicate non consecutive nodes are actually an self intersection (node 134 self intersects)
		wayWithDuplicateNonConsecutiveNodesAndIntersection = new Way();
		wayWithDuplicateNonConsecutiveNodesAndIntersection.nodes = new long[] { 150, 134, 1105, 1111, 134};
		nonIntersections = new long[] { 150, 1105, 1111};
		osm.findIntersectionNodes(wayWithDuplicateNonConsecutiveNodesAndIntersection);

		assertTrue(LongStream.of(nonIntersections)
			.allMatch(nodeid -> !osm.intersectionNodes.contains(nodeid)));
		assertTrue(osm.intersectionNodes.contains(134));

	}

	public void tearDown() throws IOException{
		Files.delete( Paths.get("./src/test/resources/tmp") );
		Files.delete( Paths.get("./src/test/resources/tmp.p") );
	}
}
