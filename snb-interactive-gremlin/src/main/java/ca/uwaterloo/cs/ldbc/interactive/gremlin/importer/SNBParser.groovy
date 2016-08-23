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


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.text.ParseException
import java.text.SimpleDateFormat


/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class SNBParser {

    static TX_MAX_RETRIES = 1000

    static void loadVertices(Graph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException, ParseException {

        String[] colNames;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat creationDateDateFormat =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String[] fileNameParts = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];

        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");
                    propertiesMap = new HashMap<>();

                    for (int j = 0; j < colVals.length; ++j) {
                        if (colNames[j].equals("id")) {
                            propertiesMap.put("iid", entityName + ":" + colVals[j]);
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

                    graph.addVertex(keyValues.toArray());

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
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
    }

    static void loadProperties(Graph graph, Path filePath, boolean printLoadingDots, int batchSize, long progReportPeriod) throws IOException {
        String[] colNames;
        String[] fileNameParts = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];

        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");

                    GraphTraversalSource g = graph.traversal();
                    Vertex vertex =
                            g.V().has("iid", entityName + ":" + colVals[0]).next();

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
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
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
        String[] fileNameParts = filePath.getFileName().toString().split("_");
        String v1EntityName = fileNameParts[0];
        String edgeLabel = fileNameParts[1];
        String v2EntityName = fileNameParts[2];

        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;

        // For progress reporting
        long startTime = System.currentTimeMillis();
        long nextProgReportTime = startTime + progReportPeriod * 1000;
        long lastLineCount = 0;

        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            while (!txSucceeded) {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");

                    GraphTraversalSource g = graph.traversal();
                    Vertex vertex1 =
                            g.V().has("iid", v1EntityName + ":" + colVals[0]).next();
                    Vertex vertex2 =
                            g.V().has("iid", v2EntityName + ":" + colVals[1]).next();

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
                        "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d",
                        (timeElapsed.intdiv(1000)).intdiv(60), (timeElapsed.intdiv(1000)) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
    }

    public static void loadSNBGraph(Graph graph, String inputBaseDir, int batchSize, long progReportPeriod) throws IOException {

        // TODO: Make file list generation programmatic. This method of loading,
        // however, will be far too slow for anything other than the very
        // smallest of SNB graphs, and is therefore quite transient. This will
        // do for now.
        List<String> nodeFiles = ["person_0_0.csv", "comment_0_0.csv", "forum_0_0.csv", "organisation_0_0.csv", "place_0_0.csv", "post_0_0.csv", "tag_0_0.csv", "tagclass_0_0.csv"]

        List<String> propertiesFiles = [
                "person_email_emailaddress_0_0.csv",
                "person_speaks_language_0_0.csv"]

        List<String> edgeFiles = [
                "comment_hasCreator_person_0_0.csv",
                "comment_hasTag_tag_0_0.csv",
                "comment_isLocatedIn_place_0_0.csv",
                "comment_replyOf_comment_0_0.csv",
                "comment_replyOf_post_0_0.csv",
                "forum_containerOf_post_0_0.csv",
                "forum_hasMember_person_0_0.csv",
                "forum_hasModerator_person_0_0.csv",
                "forum_hasTag_tag_0_0.csv",
                "organisation_isLocatedIn_place_0_0.csv",
                "person_hasInterest_tag_0_0.csv",
                "person_isLocatedIn_place_0_0.csv",
                "person_knows_person_0_0.csv",
                "person_likes_comment_0_0.csv",
                "person_likes_post_0_0.csv",
                "person_studyAt_organisation_0_0.csv",
                "person_workAt_organisation_0_0.csv",
                "place_isPartOf_place_0_0.csv",
                "post_hasCreator_person_0_0.csv",
                "post_hasTag_tag_0_0.csv",
                "post_isLocatedIn_place_0_0.csv",
                "tag_hasType_tagclass_0_0.csv",
                "tagclass_isSubclassOf_tagclass_0_0.csv"]

        try {
            for (String fileName : nodeFiles) {
                System.out.print("Loading node file " + fileName + " ");
                try {
                    loadVertices(graph, Paths.get(inputBaseDir + "/" + fileName),
                            true, batchSize, progReportPeriod);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }

            for (String fileName : propertiesFiles) {
                System.out.print("Loading properties file " + fileName + " ");
                try {
                    loadProperties(graph, Paths.get(inputBaseDir + "/" + fileName),
                            true, batchSize, progReportPeriod);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }

            for (String fileName : edgeFiles) {
                System.out.print("Loading edge file " + fileName + " ");
                try {
                    if (fileName.contains("person_knows_person")) {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), true,
                                true, batchSize, progReportPeriod);
                    } else {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), false,
                                true, batchSize, progReportPeriod);
                    }

                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            graph.close();
        }
    }

}