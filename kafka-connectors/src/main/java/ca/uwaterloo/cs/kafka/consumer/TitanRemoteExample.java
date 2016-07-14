package ca.uwaterloo.cs.kafka.consumer;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by apacaci on 5/18/16.
 */
public class TitanRemoteExample {

    private static String CONFIG_FILE = "/u4/apacaci/Projects/jimmy/titan-1.0.0-hadoop1/conf/titan-hbase-es.properties";

    public static void main(String[] args) throws ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration(CONFIG_FILE);
//        conf.setProperty("storage.backend", "hbase");
//        conf.setProperty("storage.hostname", "127.0.0.1");
//        conf.setProperty("storage.hbase.tablename", "titan");

        TitanGraph graph = TitanFactory.open(conf);

        GraphTraversal<Vertex, Vertex> vertexTraversal = graph.traversal().V();
        if(vertexTraversal.hasNext()) {
            Vertex vertex = vertexTraversal.next();
            System.out.println("Found vertex: " + vertex.label());
        }

        graph.close();

    }
}
