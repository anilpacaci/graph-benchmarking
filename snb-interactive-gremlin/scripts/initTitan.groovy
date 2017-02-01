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
import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.core.schema.SchemaAction
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */

/**
 * Helper function to handle Titan Specific initialization, i.e. schema definition and index creation
 * @param titanGraph
 */
Graph initializeTitan(String propertiesFile) {

    titanGraph = TitanFactory.open(propertiesFile)

    List<String> vertexLabels = [
            "person",
            "comment",
            "forum",
            "organisation",
            "place",
            "post",
            "tag",
            "tagclass"]

    List<String> edgeLabels = [
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
            "workAt"]

    // All property keys with Cardinality.SINGLE
    List<String> singleCardPropKeys = [
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
            "workFrom"] // workAt

    // All property keys with Cardinality.LIST
    List<String> listCardPropKeys = [
            "email", // person
            "language"] // person, post

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
        System.out.println("Exception: " + e);
        e.printStackTrace();
    }

    return titanGraph
}
