/*
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang3.tuple.Pair

import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.text.ParseException
import java.text.SimpleDateFormat

import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.VertexLabel;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class SqlgSNBImporter {

    static TX_MAX_RETRIES = 1000

    // boolean property that can be set in poperties files
    static isIdMappingEnabled = false

    // use with caution, only instantiate if isIdMappingEnabled == true
    static IDMapping idMappingServer = null

    // uses batch loading mechanism with normal batch mode
    static void loadVertices(SqlgGraph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {

        String[] colNames;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat creationDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String fileName = filePath.getFileName().toString()
        String[] fileNameParts = fileName.split("_");
        String entityName = fileNameParts[0];

        LineIterator it = FileUtils.lineIterator(filePath.toFile())
        // because very large files cannot be read into memory
        //List<String> lines = Files.readAllLines(filePath);
        colNames = it.nextLine().split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        // start tx in normal batch mode
        graph.tx().streamingWithLockBatchModeOn()

        for (int startIndex = 1; it.hasNext(); startIndex += batchSize) {
            List<String> lines = new ArrayList<>(batchSize)
            for(int i = 0 ; i < batchSize && it.hasNext() ; i++) {
                lines.add(i, it.nextLine())
            }
            int endIndex = Math.min(batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = 0; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");
                    propertiesMap = new HashMap<>();

                    String identifier = null
                    for (int j = 0; j < colVals.length; ++j) {
                        if (colNames[j].equals("id")) {
                            identifier = entityName + ":" + colVals[j]
                            propertiesMap.put("iid", identifier);
                        } else if (colNames[j].equals("birthday")) {
                            propertiesMap.put(colNames[j], String.valueOf(
                                    birthdayDateFormat.parse(colVals[j]).getTime()));
                        } else if (colNames[j].equals("creationDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(
                                    creationDateDateFormat.parse(colVals[j]).getTime()));
                        } else {
                            propertiesMap.put(colNames[j], colVals[j]);
                        }
                    }

                    propertiesMap.put(T.label, entityName);

                    List<Object> keyValues = new ArrayList<>();
                    propertiesMap.forEach { key, val ->
                        keyValues.add(key);
                        keyValues.add(val);
                    }

                    Vertex vertex = graph.addVertex(keyValues.toArray());

                    //populate IDMapping if enabled
                    if(isIdMappingEnabled) {
                        String id = vertex.id().toString()
                        if(id == null) println(String.format("%s identifier is null", identifier))
                        idMappingServer.setId(identifier, id)
//                        println(String.format("IID:%s\tID:%s", identifier, id))
                    }

                    lineCount++;
                }

                try {
                    // flush changes in current batch
                    graph.tx().flush();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }

                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format(
                            "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                    "aborting...", txFailCount, startIndex, endIndex - 1));
                }
            }

            if (printLoadingDots &&
                    (System.currentTimeMillis() > nextProgReportTime)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long linesLoaded = lineCount - lastLineCount;
                System.out.println(String.format(
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d,\tFile: %s",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded, fileName));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }

        // finally commit the entire load process
        graph.tx().commit()
        LineIterator.closeQuietly(it)

    }

    static void loadProperties(SqlgGraph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException {
        String[] colNames;
        String fileName = filePath.getFileName().toString()
        String[] fileNameParts = fileName.split("_");
        String entityName = fileNameParts[0];

        LineIterator it = FileUtils.lineIterator(filePath.toFile())
        // because very large files cannot be read into memory
        //List<String> lines = Files.readAllLines(filePath);
        colNames = it.nextLine().split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        for (int startIndex = 1; it.hasNext(); startIndex += batchSize) {
            List<String> lines = new ArrayList<>(batchSize)
            for(int i = 0 ; i < batchSize && it.hasNext() ; i++) {
                lines.add(i, it.nextLine())
            }
            int endIndex = Math.min(batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = 0; i < endIndex; i++) {
                    String line = lines.get(i);
                    println(String.format("%d:\t", lineCount))

                    String[] colVals = line.split("\\|");

                    GraphTraversalSource g = graph.traversal();
                    Vertex vertex = null
                    if(isIdMappingEnabled) {
                        String id = idMappingServer.getId(entityName + ":" + colVals[0]);
                        println(String.format("ID: %s", id))
                        vertex = g.V(id).next()
                    } else {
                        vertex = g.V().has(entityName, "iid", entityName + ":" + colVals[0]).next();
                    }

                    println(String.format("Vertex:%s", vertex.id()))
                    for (int j = 1; j < colVals.length; ++j) {
                        vertex.property(VertexProperty.Cardinality.list, colNames[j],
                                colVals[j]);
                    }

                    lineCount++;
                }

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }

                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format(
                            "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                    "aborting...", txFailCount, startIndex, endIndex - 1));
                }
            }

            if (printLoadingDots &&
                    (System.currentTimeMillis() > nextProgReportTime)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long linesLoaded = lineCount - lastLineCount;
                System.out.println(String.format(
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d,\tFile: %s",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded, fileName));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }

        //finally commit the entire load process
        graph.tx().commit()
        LineIterator.closeQuietly(it)

    }


    static void loadEdges(SqlgGraph graph, Path filePath, boolean undirected, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {
        String[] colNames;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat creationDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat joinDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String fileName = filePath.getFileName().toString()
        String[] fileNameParts = fileName.split("_");
        String v1EntityName = fileNameParts[0];
        String edgeLabel = fileNameParts[1];
        String v2EntityName = fileNameParts[2];

        LineIterator it = FileUtils.lineIterator(filePath.toFile())
        // because very large files cannot be read into memory
        //List<String> lines = Files.readAllLines(filePath);
        colNames = it.nextLine().split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        for (int startIndex = 1; it.hasNext(); startIndex += batchSize) {
            List<String> lines = new ArrayList<>(batchSize)
            for(int i = 0 ; i < batchSize && it.hasNext() ; i++) {
                lines.add(i, it.nextLine())
            }
            int endIndex = Math.min(batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = 0; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");

                    GraphTraversalSource g = graph.traversal();
                    Vertex vertex1, vertex2;

                    String iid1 = v1EntityName + ":" + colVals[0]
                    String iid2 = v2EntityName + ":" + colVals[1]

                    if(isIdMappingEnabled) {
                        String id1 = idMappingServer.getId(iid1)
                        String id2 = idMappingServer.getId(iid2)
//                        println(String.format("IID1: %s\tID1: %s\t - IID2: %s\tID2: %s", iid1, id1, iid2, id2))
                        vertex1 = g.V(id1).next()
                        vertex2 = g.V(id2).next()
                    } else {
                        vertex1 =
                                g.V().has(v1EntityName, "iid", v1EntityName + ":" + colVals[0]).next();
                        vertex2 =
                                g.V().has(v2EntityName, "iid", v2EntityName + ":" + colVals[1]).next();
                    }

                    propertiesMap = new HashMap<>();
                    for (int j = 2; j < colVals.length; ++j) {
                        if (colNames[j].equals("creationDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(
                                    creationDateDateFormat.parse(colVals[j]).getTime()));
                        } else if (colNames[j].equals("joinDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(
                                    joinDateDateFormat.parse(colVals[j]).getTime()));
                        } else {
                            propertiesMap.put(colNames[j], colVals[j]);
                        }
                    }

                    List<Object> keyValues = new ArrayList<>();
                    propertiesMap.forEach { key, val ->
                        keyValues.add(key);
                        keyValues.add(val);
                    }

                    vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());

                    if (undirected) {
                        vertex2.addEdge(edgeLabel, vertex1, keyValues.toArray());
                    }

                    lineCount++;
                }

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }

                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format(
                            "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                    "aborting...", txFailCount, startIndex, endIndex - 1));
                }
            }

            if (printLoadingDots &&
                    (System.currentTimeMillis() > nextProgReportTime)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long linesLoaded = lineCount - lastLineCount;
                System.out.println(String.format(
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d,\tFile: %s",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded, fileName));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
        LineIterator.closeQuietly(it)
        graph.tx().commit()
    }

    // uses bulk loading mechanism defined in SqlgGraph
    static void loadBulkEdges(SqlgGraph graph, Path filePath, boolean undirected, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {
        String[] colNames;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat creationDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat joinDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String[] fileNameParts = filePath.getFileName().toString().split("_");
        String v1EntityName = fileNameParts[0];
        String edgeLabel = fileNameParts[1];
        String v2EntityName = fileNameParts[2];

        LineIterator it = FileUtils.lineIterator(filePath.toFile())
        // because very large files cannot be read into memory
        //List<String> lines = Files.readAllLines(filePath);
        colNames = it.nextLine().split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;


        graph.tx().streamingBatchModeOn();
        for (int startIndex = 1; it.hasNext(); startIndex += batchSize) {
            List<String> lines = new ArrayList<>(batchSize)
            for(int i = 0 ; i < batchSize && it.hasNext() ; i++) {
                lines.add(i, it.nextLine())
            }
            int endIndex = Math.min(batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                List<Pair<String, String>> identifiers = new ArrayList<>()
                for (int i = 0; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");


                    identifiers.add(Pair.of(v1EntityName + ":" + colVals[0], v2EntityName + ":" + colVals[1]))

                    lineCount++;
                }
                graph.bulkAddEdges(v1EntityName, v2EntityName, edgeLabel, Pair.of("iid", "iid"), identifiers )

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                    graph.tx().streamingBatchModeOn();
                } catch (Exception e) {
                    txFailCount++;
                }

                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format(
                            "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                    "aborting...", txFailCount, startIndex, endIndex - 1));
                }
            }

            if (printLoadingDots &&
                    (System.currentTimeMillis() > nextProgReportTime)) {
                long timeElapsed = System.currentTimeMillis() - startTime;
                long linesLoaded = lineCount - lastLineCount;
                System.out.println(String.format(
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }

        // finally commit the entire load process
        graph.tx().commit()
        LineIterator.closeQuietly(it)
    }

    public static SqlgGraph initializeSqlgGraph(String propertiesFile) {
        SqlgGraph graph = SqlgGraph.open(propertiesFile)

        List<String> vertexLabels = [
                "person",
                "comment",
                "forum",
                "organisation",
                "place",
                "post",
                "tag",
                "tagclass"]

        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("iid", PropertyType.STRING);
        }}

        Set<PropertyColumn> propertyColumns = new HashSet();

        // now create indexes for SNB Graph
        for(String vlabel : vertexLabels ) {
            VertexLabel vertexLabel = graph.getTopology().getPublicSchema().ensureVertexLabelExist(vlabel, properties)
            PropertyColumn iid = vertexLabel.getProperty("iid").get();
            propertyColumns.add(iid)
            graph.tx().commit()
        }

        graph.getTopology().ensureGlobalUniqueIndexExist(propertyColumns)
        graph.tx().commit();

        return graph
    }

    public static void loadSNBGraph(SqlgGraph graph, String inputBaseDir, String configurationFile, int batchSize, long progReportPeriod) throws IOException {

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        String[] nodeFiles = configuration.getStringArray("nodes")
        String[] propertiesFiles = configuration.getStringArray("properties")
        String[] edgeFiles = configuration.getStringArray("edges")

        List<Thread> threads = new ArrayList<>()

        isIdMappingEnabled = configuration.getBoolean("id.mapping", false)

        if(isIdMappingEnabled) {
            String[] servers = configuration.getStringArray("memcached.address")
            idMappingServer = new IDMapping(servers)
        }

        try {
            for (String fileName : nodeFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.VERTEX, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.run()
            }

//            for(Thread thread : threads) {
//                thread.join()
//            }
            threads.clear()

            for (String fileName : propertiesFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.PROPERTY, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.start()
            }

            for(Thread thread : threads) {
                thread.join()
            }
            threads.clear()

            for (String fileName : edgeFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.EDGE, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.start()
            }

            for(Thread thread : threads) {
                thread.join()
            }
            threads.clear()

        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            graph.close();
        }
    }

    static class IDMapping {

        private static String INSTANCE_NAME = "id-mapping";

        private MemCachedClient client;

        IDMapping(String... servers) {
            SockIOPool pool = SockIOPool.getInstance(INSTANCE_NAME);
            pool.setServers(servers);
            pool.setFailover(true);
            pool.setInitConn(10);
            pool.setMinConn(5);
            pool.setMaxConn(250);
            pool.setMaintSleep(30);
            pool.setNagle(false);
            pool.setSocketTO(3000);
            pool.setAliveCheck(true);
            pool.initialize();

            client = new MemCachedClient(INSTANCE_NAME);
            client.flushAll();
        }

        String getId(String identifier) {
            Object value = client.get(identifier);
            if (value == null)
                return null;
            return (String) value;
        }

        void setId(String identifier, String id) {
            client.set(identifier, id)
        }
    }

    static class LoadTask implements Runnable {

        private String fileName;
        private ElementType elementType;

        private SqlgGraph graph
        private String inputBaseDir
        private int batchSize
        private long reportingPeriod

        LoadTask(String fileName, ElementType elementType, SqlgGraph graph, String inputBaseDir, int batchSize, long reportingPeriod) {
            this.fileName = fileName
            this.elementType = elementType
            this.graph = graph
            this.inputBaseDir = inputBaseDir
            this.batchSize = batchSize
            this.reportingPeriod = reportingPeriod
        }

        @Override
        void run() {

            if(elementType.equals(ElementType.VERTEX)) {
                System.out.println("Loading node file " + fileName + " ");
                try {
                    loadVertices(   graph, Paths.get(inputBaseDir + "/" + fileName),
                            true, batchSize, reportingPeriod);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            } else if(elementType.equals(ElementType.PROPERTY)) {
                System.out.println("Loading properties file " + fileName + " ");
                try {
                    loadProperties(graph, Paths.get(inputBaseDir + "/" + fileName),
                            true, batchSize, reportingPeriod);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            } else {
                System.out.println("Loading edge file " + fileName + " ");
                try {
                    if (fileName.contains("person_knows_person")) {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), true,
                                true, batchSize, reportingPeriod);
                    } else {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), false,
                                true, batchSize, reportingPeriod);
                    }

                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }

        }
    }
    
    enum ElementType {VERTEX, PROPERTY, EDGE}

}