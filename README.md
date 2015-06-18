# osm-lib

A library for random entity access inside OSM files of any size, up to and including `planet.pbf`.

## Build the JAR

`$ mvn clean package`

## Run Vanilla Extract

`$ mvn exec:java -Dexec.mainClass="com.conveyal.osmlib.VanillaExtract" -Dexec.args=...`

### Replicate the planet or a smaller region

You'll want to load from a PBF source that contains a timestamp if you want minutely updates. `Planet` mirrors,
Mapzen Metro Extracts, and Geofabrik extracts contain timestamps. Watch for the timestamp to appear as the load
process begins just to make sure it was properly read.

`VanillaExtract /mnt/ssd2/vexdata --loadURL http://ftp.snt.utwente.nl/pub/misc/openstreetmap/planet-latest.osm.pbf`

`VanillaExtract vex.data --loadURL https://s3.amazonaws.com/metro-extracts.mapzen.com/aarhus_denmark.osm.pbf`

`VanillaExtract /mnt/ssd2/vexdata --load /home/abyrd/france.osm.pbf`

`VanillaExtract /mnt/ssd2/vexdata --load /home/abyrd/belgium.vex`

### Run an extract server with minutely updates

`VanillaExtract /mnt/ssd2/vexdata`

## Fetch a geographic extract

### Near Aarhus, Denmark as PBF

`wget http://localhost:9001/56.12761,10.056558,56.179451,10.144608.pbf`

### Near Aarhus, Denmark in VEX format

`wget http://localhost:9001/56.12761,10.056558,56.179451,10.144608.vex`
