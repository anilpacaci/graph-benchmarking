package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import com.ldbc.driver.DbConnectionState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class GremlinKafkaDbConnectionState extends DbConnectionState {

    final static Logger logger = LoggerFactory.getLogger(GremlinKafkaDbConnectionState.class);
    private static String CONFIG_FILE = "producer.properties";
    private static String KAFKA_TOPIC = "ldbc_updates";

    private Cluster cluster;
    private Client remoteClient;

    private KafkaProducer<String, GremlinStatement> producer;

    private void setUpKafka() {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = GremlinKafkaDbConnectionState.class.getClassLoader().getResourceAsStream( CONFIG_FILE );
            if (input == null) {
                System.out.println( "Sorry, unable to find " + CONFIG_FILE );
                return;
            }
            //load a properties file from class path, inside static method
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        producer = new KafkaProducer<String, GremlinStatement>(prop);
    }

    public GremlinKafkaDbConnectionState(Map<String, String> properties) {
        String locator;
        if (properties.containsKey("locator")) {
            locator = properties.get("locator");
        } else {
            locator = "127.0.0.1";
        }

        try {
            cluster = Cluster.open(locator);
            remoteClient = cluster.connect();
        } catch (Exception e) {
            logger.error("Connection to remote Gremlin Server could NOT obtained");
        }
        setUpKafka();
    }

    /**
     * Cluster/Client is configured through constructor. Just a utility method to retrieve client reference
     * @return Client for Remote Gremlin Server
     */
    public Client getClient() {
        return remoteClient;
    }

    public KafkaProducer<String, GremlinStatement> getKafkaProducer() {
        return producer;
    }

    public String getKafkaTopic() {
        return KAFKA_TOPIC;
    }

    @Override
    public void close() throws IOException {
        remoteClient.close();
        cluster.close();
    }
}
