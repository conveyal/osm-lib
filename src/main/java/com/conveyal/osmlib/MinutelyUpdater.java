package com.conveyal.osmlib;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class MinutelyUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(MinutelyUpdater.class);

    public static final String BASE_URL = "http://planet.openstreetmap.org/replication/";

    OSM osm = new OSM(null);

    long dbTimestamp = new GregorianCalendar(2015, 4, 6).getTimeInMillis() / 1000;

    public void runUpdate() {
        long now = System.currentTimeMillis() / 1000;
        // long timestamp = osm.db.getAtomicLong("timestamp").get(); // UTC
        // If more than one year ago, complain. If more than a few minutes in the future, complain.
        for (String timescale : Arrays.asList("day", "hour", "minute")) {
            List<DiffState> workQueue = new ArrayList<DiffState>();
            DiffState scaleState = fetchState(timescale, 0);
            // Working backward, find all updates that are dated after the current database timestamp
            for (int seq = scaleState.sequenceNumber; seq > 0; seq--) {
                DiffState state = fetchState(timescale, seq);
                if (state.timestamp < dbTimestamp) break;
                workQueue.add(state);
            }
            LOG.info("Applying {} {} updates.", workQueue.size(), timescale);
            // Put the updates in chronological order and apply them
            applyDiffs(Lists.reverse(workQueue));
        }

    }

    public static class DiffState {
        URL url;
        String timescale;
        int sequenceNumber;
        long timestamp;

        @Override
        public String toString() {
            return "DiffState " +
                    "sequenceNumber=" + sequenceNumber +
                    ", timestamp=" + timestamp +
                    ", url=" + url;
        }
    }

    public DiffState fetchState(String timescale, int sequenceNumber) {
        DiffState diffState = new DiffState();
        try {
            StringBuilder sb = new StringBuilder(BASE_URL);
            sb.append(timescale);
            sb.append("/");
            if (sequenceNumber > 0) {
                int a = sequenceNumber / 1000000;
                int b = (sequenceNumber - (a * 1000000)) / 1000;
                int c = (sequenceNumber - (a * 1000000) - (b * 1000));
                sb.append(String.format(Locale.US, "%03d/%03d/%03d", a, b, c));
                // Record the URL of the changeset itself
                sb.append(".osc.gz");
                diffState.url = new URL(sb.toString());
                // Remove the changeset filename, leaving dot
                sb.delete(sb.length() - 6, sb.length());
            } else {
                LOG.info("Checking replication state for timescale {}", timescale);
            }
            sb.append("state.txt");
            URL url = new URL(sb.toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String line;
            Map<String, String> kvs = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] fields = line.split("=");
                if (fields.length != 2) continue;
                kvs.put(fields[0], fields[1]);
            }
            String dateTimeString = kvs.get("timestamp").replace("\\:", ":");
            diffState.timestamp = DatatypeConverter.parseDateTime(dateTimeString).getTimeInMillis() / 1000;
            diffState.sequenceNumber = Integer.parseInt(kvs.get("sequenceNumber"));
            diffState.timescale = timescale;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        // LOG.info("state {}", diffState);
        return diffState;
    }

    public static void main(String[] args) {
        MinutelyUpdater updater = new MinutelyUpdater();
        updater.runUpdate();
    }

    public String getDateString(long secondsSinceEpoch) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return format.format(new Date(secondsSinceEpoch));
    }

    public void applyDiffs(List<DiffState> workQueue) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
//            factory.setNamespaceAware(true);
            SAXParser saxParser = factory.newSAXParser();
            DefaultHandler handler = new ChangesetParser(osm);
            for (DiffState state : workQueue) {
                LOG.info("Applying {} update for {}", state.timescale, getDateString(state.timestamp * 1000));
                InputStream inputStream = new GZIPInputStream(state.url.openStream());
                saxParser.parse(inputStream, handler);
                // Move the DB timestamp forward to that of the update that was applied
                dbTimestamp = state.timestamp;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}