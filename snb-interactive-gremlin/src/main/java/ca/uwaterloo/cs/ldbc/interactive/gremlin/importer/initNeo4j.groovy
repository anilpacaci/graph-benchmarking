package ca.uwaterloo.cs.ldbc.interactive.gremlin.importer
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

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph


/**
 * Helper function to handle Neo4j Specific initialization, i.e. schema definition and index creation
 * @param neo4jGraph
 */
public static void initializeNeo4j(Neo4jGraph neo4jGraph) {
    List<String> vertexLabels = [
            "person",
            "comment",
            "forum",
            "organisation",
            "place",
            "post",
            "tag",
            "tagclass"]

    vertexLabels.forEach { label ->
        neo4jGraph.cypher("CREATE INDEX ON :" + label + "(iid)")
        neo4jGraph.tx().commit()

    }
}