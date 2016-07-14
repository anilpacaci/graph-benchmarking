package ca.uwaterloo.cs.kafka.producer;

import ca.uwaterloo.cs.kafka.utils.GraphEdge;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by apacaci on 5/16/16.
 */
public class GraphEdgeProducer {

    private static String CONFIG_FILE = "producer.properties";
    private static String INPUT_GRAPH_FILE = "/u4/apacaci/Projects/jimmy/datasets/cit-Patents.txt";

    public static void main(String[] args) {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = GraphEdgeProducer.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
            if(input==null){
                System.out.println("Sorry, unable to find " + CONFIG_FILE);
                return;
            }

            //load a properties file from class path, inside static method
            prop.load(input);
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

        //now we can construct the KafkaProducer
        Producer<String, GraphEdge> producer = new KafkaProducer<>(prop);

        // now we need to access the graph File
        File inputGraphFile = new File(INPUT_GRAPH_FILE);
        LineIterator lineIterator= null;
        try {
            lineIterator = FileUtils.lineIterator(inputGraphFile);

            int edgeCounter = 0;
            while(lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                if(line == null || line.isEmpty() || line.startsWith("#")) {
                    // not a valid line, dont process it
                    continue;
                }

                // now process the line and publish it to Kafka
                GraphEdge edge = processLine(line);
                producer.send(new ProducerRecord<String, GraphEdge>("topic1", Integer.toString(edgeCounter), edge));
                edgeCounter++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lineIterator.close();
        }

    }

    private static GraphEdge processLine(String line) {
        // in case of CIT Patents, each line is an edge with ids of two endpoint
        String[] vertices = line.split("\\s+");
        GraphEdge edge = new GraphEdge(vertices[0], vertices[1]);
        return edge;
    }
}
