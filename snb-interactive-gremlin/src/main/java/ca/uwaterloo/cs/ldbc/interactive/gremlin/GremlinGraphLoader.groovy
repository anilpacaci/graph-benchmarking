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


import com.thinkaurelius.titan.core.Cardinality
import com.thinkaurelius.titan.core.Multiplicity
import com.thinkaurelius.titan.core.PropertyKey
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.core.schema.SchemaAction
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.logging.Level
import java.util.logging.Logger

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
public class GremlinGraphLoader {

    private static final Logger logger =
            Logger.getLogger(GremlinGraphLoader.class.getName());

    private static final long TX_MAX_RETRIES = 1000;

    public static void loadVertices(Graph graph, Path filePath,
                                    boolean printLoadingDots, int batchSize, long progReportPeriod)
            throws IOException, java.text.ParseException {

        String[] colNames = null;
        boolean firstLine = true;
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
                        (timeElapsed / 1000) / 60, (timeElapsed / 1000) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
    }

    public static void loadProperties(Graph graph, Path filePath,
                                      boolean printLoadingDots, int batchSize, long progReportPeriod)
            throws IOException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
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
                        (timeElapsed / 1000) / 60, (timeElapsed / 1000) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
    }

    public static void loadEdges(Graph graph, Path filePath, boolean undirected,
                                 boolean printLoadingDots, int batchSize, long progReportPeriod)
            throws IOException, java.text.ParseException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
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
                        (timeElapsed / 1000) / 60, (timeElapsed / 1000) % 60, linesLoaded));
                nextProgReportTime += progReportPeriod * 1000;
                lastLineCount = lineCount;
            }
        }
    }

    /**
     * Helper function to handle Neo4j Specific initialization, i.e. schema definition and index creation
     * @param neo4jGraph
     */
    public static void initializeNeo4j(Neo4jGraph neo4jGraph) {
        String[] vertexLabels = [
                "person",
                "comment",
                "forum",
                "organisation",
                "place",
                "post",
                "tag",
                "tagclass"
        ]

        vertexLabels.forEach { label ->
            neo4jGraph.cypher("CREATE INDEX ON :" + label + "(iid)")
            neo4jGraph.tx().commit()

        }
    }

    /**
     * Helper function to handle Titan Specific initialization, i.e. schema definition and index creation
     * @param titanGraph
     */
    public static void initializeTitan(TitanGraph titanGraph) {

        String[] vertexLabels = [
                "person",
                "comment",
                "forum",
                "organisation",
                "place",
                "post",
                "tag",
                "tagclass"
        ]

        String[] edgeLabels = [
                "containerOf",
                "hasCreator",
                "hasInterest",
                "hasMember",
                "hasModerator",
                "hasTag",
                "hasType",
                "isLocatedIn",
                "isPartOf",
                "isSubclassOf",
                "knows",
                "likes",
                "replyOf",
                "studyAt",
                "workAt"
        ]

        // All property keys with Cardinality.SINGLE
        String[] singleCardPropKeys = [
                "birthday", // person
                "browserUsed", // comment person post
                "classYear", // studyAt
                "content", // comment post
                "creationDate", // comment forum person post knows likes
                "firstName", // person
                "gender", // person
                "imageFile", // post
                "joinDate", // hasMember
                //"language", // post
                "lastName", // person
                "length", // comment post
                "locationIP", // comment person post
                "name", // organisation place tag tagclass
                "title", // forum
                "type", // organisation place
                "url", // organisation place tag tagclass
                "workFrom", // workAt
        ]

        // All property keys with Cardinality.LIST
        String[] listCardPropKeys = [
                "email", // person
                "language" // person, post
        ]

        /*
     * Explicitly define the graph schema.
     *
     * Note: For unknown reasons, it seems that each modification to the
     * schema must be committed in its own transaction.
     */
        try {
            ManagementSystem mgmt;

            // Declare all vertex labels.
            for (String vLabel : vertexLabels) {
                System.out.println(vLabel);
                mgmt = (ManagementSystem) titanGraph.openManagement();
                mgmt.makeVertexLabel(vLabel).make();
                mgmt.commit();
            }

            // Declare all edge labels.
            for (String eLabel : edgeLabels) {
                System.out.println(eLabel);
                mgmt = (ManagementSystem) titanGraph.openManagement();
                mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
                mgmt.commit();
            }

            // Delcare all properties with Cardinality.SINGLE
            for (String propKey : singleCardPropKeys) {
                System.out.println(propKey);
                mgmt = (ManagementSystem) titanGraph.openManagement();
                mgmt.makePropertyKey(propKey).dataType(String.class)
                        .cardinality(Cardinality.SINGLE).make();
                mgmt.commit();
            }

            // Delcare all properties with Cardinality.LIST
            for (String propKey : listCardPropKeys) {
                System.out.println(propKey);
                mgmt = (ManagementSystem) titanGraph.openManagement();
                mgmt.makePropertyKey(propKey).dataType(String.class)
                        .cardinality(Cardinality.LIST).make();
                mgmt.commit();
            }

            /*
             * Create a special ID property where we will store the IDs of
             * vertices in the SNB dataset, and a corresponding index. This is
             * necessary because TitanDB generates its own IDs for graph
             * vertices, but the benchmark references vertices by the ID they
             * were originally assigned during dataset generation.
             */
            mgmt = (ManagementSystem) titanGraph.openManagement();
            mgmt.makePropertyKey("iid").dataType(String.class)
                    .cardinality(Cardinality.SINGLE).make();
            mgmt.commit();

            mgmt = (ManagementSystem) titanGraph.openManagement();
            PropertyKey iid = mgmt.getPropertyKey("iid");
            mgmt.buildIndex("byIid", Vertex.class).addKey(iid).buildCompositeIndex();
            mgmt.commit();

            mgmt.awaitGraphIndexStatus(titanGraph, "byIid").call();

            mgmt = (ManagementSystem) titanGraph.openManagement();
            mgmt.updateIndex(mgmt.getGraphIndex("byIid"), SchemaAction.REINDEX)
                    .get();
            mgmt.commit();

        } catch (Exception e) {
            logger.log(Level.SEVERE, e.toString());
            return;
        }


    }

    public
    static void loadSNBGraph(Graph graph, String inputBaseDir, int batchSize, long progReportPeriod) throws IOException {

        // TODO: Make file list generation programmatic. This method of loading,
        // however, will be far too slow for anything other than the very
        // smallest of SNB graphs, and is therefore quite transient. This will
        // do for now.
        String[] nodeFiles = [
                "person_0_0.csv",
                "comment_0_0.csv",
                "forum_0_0.csv",
                "organisation_0_0.csv",
                "place_0_0.csv",
                "post_0_0.csv",
                "tag_0_0.csv",
                "tagclass_0_0.csv"
        ]

        String[] propertiesFiles = [
                "person_email_emailaddress_0_0.csv",
                "person_speaks_language_0_0.csv"
        ]

        String[] edgeFiles = [
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
                "tagclass_isSubclassOf_tagclass_0_0.csv"
        ]

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
