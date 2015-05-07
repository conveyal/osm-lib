package com.conveyal.osmlib;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * TODO rename to UpdateParser or DiffParser
 */
public class ChangesetParser extends DefaultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ChangesetParser.class);

    OSM osm;
    boolean inDelete = false; // if false, assume we're in add or modify
    OSMEntity entity;
    long id;
    int nParsed = 0;
    TLongList nodeRefs;

    public ChangesetParser(OSM osm) {
        this.osm = osm;
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {

        String idString = attributes.getValue("id");
        id = idString == null ? -1 : Long.parseLong(idString);

        if (qName.equalsIgnoreCase("ADD") || qName.equalsIgnoreCase("MODIFY")) {
            inDelete = false;
        } else if (qName.equalsIgnoreCase("DELETE")) {
            inDelete = true;
        } else if (qName.equalsIgnoreCase("NODE")) {
            Node node = new Node();
            double lat = Double.parseDouble(attributes.getValue("lat"));
            double lon = Double.parseDouble(attributes.getValue("lon"));
            node.setLatLon(lat, lon);
            entity = node;
        } else if (qName.equalsIgnoreCase("WAY")) {
            Way way = new Way();
            entity = way;
            nodeRefs = new TLongArrayList();
        } else if (qName.equalsIgnoreCase("RELATION")) {
            Relation relation = new Relation();
            entity = relation;
        } else if (qName.equalsIgnoreCase("TAG")) {
            entity.addTag(attributes.getValue("k"), attributes.getValue("v"));
        } else if (qName.equalsIgnoreCase("ND")) {
            nodeRefs.add(Long.parseLong(attributes.getValue("ref")));
        }
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {

        nParsed++;
        if (nParsed % 1000000 == 0) {
            LOG.info(" {}M applied", nParsed / 1000000);
        }
        if (qName.equalsIgnoreCase("DELETE")) {
            inDelete = false;
            return;
        }

        if (qName.equalsIgnoreCase("NODE")) {
            if (inDelete) {
                osm.nodes.remove(id);
            } else {
                osm.nodes.put(id, (Node) entity);
            }
        } else if (qName.equalsIgnoreCase("WAY")) {
            if (inDelete) {
                osm.ways.remove(id);
            } else {
                Way way = ((Way)entity);
                way.nodes = nodeRefs.toArray();
                osm.ways.put(id, way);
            }
        } else if (qName.equalsIgnoreCase("RELATION")) {
            if (inDelete) {
                osm.relations.remove(id);
            } else {
                osm.relations.put(id, (Relation) entity);
            }
        }
    }

    public void characters(char ch[], int start, int length) throws SAXException { }

}