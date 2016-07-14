package ca.uwaterloo.cs.kafka.consumer;

import ca.uwaterloo.cs.kafka.utils.GraphEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by apacaci on 5/16/16.
 */
public class TitanEdgeConsumer {

    private static String KAFKA_CONFIG_FILE = "consumer.properties";
    private static String TITAN_CONFIG_FILE = "titan-cassandra-es.properties";

    public static void main(String[] args) {

        Properties kafkaConf = new Properties();
        InputStream input = null;

        // read TitanConsumer configuration from a file
        try {
            input = TitanEdgeConsumer.class.getClassLoader().getResourceAsStream(KAFKA_CONFIG_FILE);
            if(input==null){
                System.out.println("Sorry, unable to find " + KAFKA_CONFIG_FILE);
                return;
            }

            //load a properties file from class path, inside static method
            kafkaConf.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //now we can construct the KafkaConsumer
        KafkaConsumer<String, GraphEdge> consumer = new KafkaConsumer<>(kafkaConf);
        consumer.subscribe(Arrays.asList("topic1"));

        // connect Titan
        PropertiesConfiguration titanConf = null;
        try {
            titanConf = new PropertiesConfiguration(TITAN_CONFIG_FILE);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        TitanGraph graph = TitanFactory.open(titanConf);


        // start consuming messages from Kafka Topic, add edges to the graph
        while (true) {
            ConsumerRecords<String, GraphEdge> records = consumer.poll(100);
            for (ConsumerRecord<String, GraphEdge> record : records) {
                GraphEdge edge = record.value();
                System.out.printf("offset = %d, key = %s, vertex1 = %s, vertex2 = %s \n", record.offset(), record.key(), edge.getSourceVertex(), edge.getTargetVertex());
                Vertex sourceVertex = getOrCreateVertex(graph, edge.getSourceVertex());
                Vertex targetVertex = getOrCreateVertex(graph, edge.getTargetVertex());
                sourceVertex.addEdge(Long.toString(record.offset()), targetVertex);
                graph.tx().commit();
            }
        }

    }

    /**
     * For a given nodeID, it returns a vertex with given nodeID as a property
     * If such vertex does not exists, it creates new one
     * @param graph
     * @param nodeID
     * @return
     */
    private static Vertex getOrCreateVertex(TitanGraph graph, String nodeID) {

        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has("nodeID", nodeID);
        if(traversal.hasNext()) {
            return traversal.next();
        }
        // Vertex does not exist, create it
        Vertex vertex = graph.addVertex();
        vertex.property("nodeID", nodeID);
        graph.tx().commit();

        return vertex;
    }

    /**
     * For a given nodeID, it returns a vertex with given ID
     * If such vertex does not exists, it creates new one
     * @param graph
     * @param nodeID
     * @return
     */
    private static Vertex getOrCreateVertexWithID(TitanGraph graph, String nodeID) {

        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasId(nodeID);
        if(traversal.hasNext()) {
            return traversal.next();
        }
        // Vertex does not exist, create it
        Vertex vertex = graph.newTransaction().addVertex(Long.valueOf(nodeID));
        graph.tx().commit();

        return vertex;
    }
}
