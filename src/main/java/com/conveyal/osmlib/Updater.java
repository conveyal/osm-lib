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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class Updater implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

    public static final String BASE_URL = "http://planet.openstreetmap.org/replication/";

    private static final Instant MIN_REPLICATION_INSTANT = Instant.parse("2015-05-01T00:00:00.00Z");

    private static final Instant MAX_REPLICATION_INSTANT = Instant.parse("2100-02-01T00:00:00.00Z");

    OSM osm;

    DiffState lastApplied;

    public Updater(OSM osm) {
        this.osm = osm;
    }

    public static enum Timescale {
        DAY, HOUR, MINUTE;
        public String lowerCase() {
            return this.toString().toLowerCase();
        }
    }

    /** Rename to just diff, represents one diff. */
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
                LOG.debug("Checking replication state for timescale {}", timescale);
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

    public String getDateString(long secondsSinceEpoch) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return format.format(new Date(secondsSinceEpoch));
    }

    /**
     * @return a chronologically ordered list of all diffs at the given timescale with a timestamp after
     * the database timestamp.
     */
    public List<DiffState> findDiffs (String timescale) {
        List<DiffState> workQueue = new ArrayList<DiffState>();
        DiffState latest = fetchState(timescale, 0);
        // Only check specific updates if the overall state for this timescale implies there are new ones.
        if (latest.timestamp > osm.timestamp.get()) {
            // Working backward, find all updates that are dated after the current database timestamp.
            for (int seq = latest.sequenceNumber; seq > 0; seq--) {
                DiffState diff = fetchState(timescale, seq);
                if (diff.timestamp <= osm.timestamp.get()) break;
                workQueue.add(diff);
            }
        }
        LOG.info("Found {} {}-scale updates.", workQueue.size(), timescale);
        // Put the updates in chronological order before returning them
        return Lists.reverse(workQueue);
    }

    public void applyDiffs(List<DiffState> workQueue) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            SAXParser saxParser = factory.newSAXParser();
            DefaultHandler handler = new OSMChangeParser(osm);
            for (DiffState state : workQueue) {
                LOG.info("Applying {} update for {}", state.timescale, getDateString(state.timestamp * 1000));
                InputStream inputStream = new GZIPInputStream(state.url.openStream());
                saxParser.parse(inputStream, handler);
                // Move the DB timestamp forward to that of the update that was applied
                osm.timestamp.set(state.timestamp);
                // Record the last update applied so we can jump straight to the next one
                lastApplied = state;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // If we have a last update, add one to its seq number and attempt to fetch
    // Define order on updates based on timestamp.

    /**
     * This is the main entry point. Give it an OSM database and it will keep it up to date in a new thread.
     */
    public static Thread spawnUpdateThread(OSM osm) {
        Thread updateThread = new Thread(new Updater(osm));
        Instant initialTimestamp = Instant.ofEpochSecond(osm.timestamp.get());
        if (initialTimestamp.isBefore(MIN_REPLICATION_INSTANT) || initialTimestamp.isAfter(MAX_REPLICATION_INSTANT)) {
            LOG.error("OSM database timestamp seems incorrect: {}", initialTimestamp.toString());
            LOG.error("Not running the minutely updater thread.");
        } else {
            updateThread.start();
        }
        return updateThread;
    }

    public static void main(String[] args) {
        spawnUpdateThread(new OSM(null));
    }

    /** Run the updater, usually in another thread. */
    @Override
    public void run() {
        while (true) {
            // long timestamp = osm.db.getAtomicLong("timestamp").get(); // UTC
            // If more than one year ago, complain. If more than a few minutes in the future, complain.
            long now = System.currentTimeMillis() / 1000;
            if ((now - osm.timestamp.get()) > 60 * 60 * 24) {
                applyDiffs(findDiffs("day"));
            }
            if ((now - osm.timestamp.get()) > 60 * 60) {
                applyDiffs(findDiffs("hour"));
            }
            applyDiffs(findDiffs("minute"));
            // Attempt to lock polling phase to server updates
            // TODO use time that file actually appeared rather than database timestamp
            int phaseErrorSeconds = (int) (((System.currentTimeMillis() / 1000) - lastApplied.timestamp) % 60);
            phaseErrorSeconds -= 5; // adjust for expected database export latency
            if (Math.abs(phaseErrorSeconds) > 1) {
                LOG.info("Compensating for polling phase error of {} seconds", phaseErrorSeconds);
            }
            int sleepSeconds = 60 - phaseErrorSeconds; // reduce 1-minute polling wait by phase difference
            if (sleepSeconds > 1) {
                LOG.debug("Sleeping {} seconds", sleepSeconds);
                try {
                    Thread.sleep(sleepSeconds * 1000);
                } catch (InterruptedException e) {
                    LOG.info("Thread interrupted, exiting polling loop.");
                    break;
                }
            }
        }
    }

}