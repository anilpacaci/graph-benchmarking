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
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty

import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.text.ParseException
import java.text.SimpleDateFormat

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class SNBParser {

    static TX_MAX_RETRIES = 1000

    static isIdMappingEnabled = false

    static IDMapping idMappingServer = null

    static void loadVertices(Graph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {

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

        LineIterator it = FileUtils.lineIterator(filePath.toFile(), "UTF-8")
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
                    propertiesMap = new HashMap<>();

                    String identifier;
                    for (int j = 0; j < colVals.length; ++j) {
                        if (colNames[j].equals("id")) {
                            identifier = entityName + ":" + colVals[j]
                            propertiesMap.put("iid", identifier);
                            propertiesMap.put("iid_long", Long.parseLong(colVals[j]))
                        } else if (colNames[j].equals("birthday")) {
                            propertiesMap.put(colNames[j], birthdayDateFormat.parse(colVals[j]).getTime());
                        } else if (colNames[j].equals("creationDate")) {
                            propertiesMap.put(colNames[j], creationDateDateFormat.parse(colVals[j]).getTime());
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
                        Long id = (Long) vertex.id()
                        idMappingServer.setId(identifier, id)
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

    }

    static void loadProperties(Graph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException {
        String[] colNames;
        String fileName = filePath.getFileName().toString()
        String[] fileNameParts = fileName.split("_");
        String entityName = fileNameParts[0];

        LineIterator it = FileUtils.lineIterator(filePath.toFile(), "UTF-8")
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

                    Vertex vertex = null;
                    if(isIdMappingEnabled) {
                        Long id = idMappingServer.getId(entityName + ":" + colVals[0]);
                        vertex = g.V(id).next()
                    } else {
                        vertex = g.V().has(entityName, "iid", entityName + ":" + colVals[0]).next();
                    }

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
        LineIterator.closeQuietly(it)

    }

    static void loadEdges(Graph graph, Path filePath, boolean undirected, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {
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

        LineIterator it = FileUtils.lineIterator(filePath.toFile(), "UTF-8")
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

                    if(isIdMappingEnabled) {
                        Long id1 = idMappingServer.getId(v1EntityName + ":" + colVals[0])
                        Long id2 = idMappingServer.getId(v2EntityName + ":" + colVals[1])
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
                            propertiesMap.put(colNames[j], creationDateDateFormat.parse(colVals[j]).getTime());
                        } else if (colNames[j].equals("joinDate")) {
                            propertiesMap.put(colNames[j], joinDateDateFormat.parse(colVals[j]).getTime());
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
    }

    public static void loadSNBGraph(Graph graph, String inputBaseDir, String configurationFile, int batchSize, long progReportPeriod) throws IOException {

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        String[] nodeFiles = configuration.getStringArray("nodes")
        String[] propertiesFiles = configuration.getStringArray("properties")
        String[] edgeFiles = configuration.getStringArray("edges")

        isIdMappingEnabled = configuration.getBoolean("id.mapping")

        if(isIdMappingEnabled) {
            String[] servers = configuration.getStringArray("memcached.address")
            idMappingServer = new IDMapping(servers)
        }

        List<Thread> threads = new ArrayList<>()

        try {
            for (String fileName : nodeFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.VERTEX, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.run()
            }

            for(Thread thread : threads) {
                thread.join()
            }
            threads.clear()

            for (String fileName : propertiesFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.PROPERTY, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.run()
            }

            for(Thread thread : threads) {
                thread.join()
            }
            threads.clear()

            for (String fileName : edgeFiles) {
                LoadTask t = new LoadTask(fileName, ElementType.EDGE, graph, inputBaseDir, batchSize, progReportPeriod)
                Thread thread = new Thread(t)
                threads.add(thread)
                thread.run()
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

        public IDMapping(String... servers) {
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

        public Long getId(String identifier) {
            Object value = client.get(identifier);
            if (value == null)
                return null;
            return (Long) value;
        }

        public void setId(String identifier, Long id) {
            client.set(identifier, id)
        }
    }

    static class LoadTask implements Runnable {

        private String fileName;
        private ElementType elementType;

        private Graph graph
        private String inputBaseDir
        private int batchSize
        private long reportingPeriod

        LoadTask(String fileName, ElementType elementType, Graph graph, String inputBaseDir, int batchSize, long reportingPeriod) {
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
