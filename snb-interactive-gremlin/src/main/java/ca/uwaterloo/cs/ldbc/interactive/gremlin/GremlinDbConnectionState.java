package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.handler.GremlinUpdateHandler;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.handler.KafkaUpdateHandler;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.handler.UpdateHandler;
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

public class GremlinDbConnectionState extends DbConnectionState {

    final static Logger logger = LoggerFactory.getLogger(GremlinDbConnectionState.class);
    private static String KAFKA_TOPIC = "ldbc_updates";

    private Cluster cluster;
    private Client remoteClient;

    private KafkaProducer<String, GremlinStatement> producer = null;

    private UpdateHandler updateHandler;

    private void setUpKafka(String config_file) {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = GremlinDbConnectionState.class.getClassLoader().getResourceAsStream(config_file);
            if (input == null) {
                System.out.println( "Sorry, unable to find " + config_file);
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

    public GremlinDbConnectionState( Map<String, String> properties) {
        String backend;
        if (properties.containsKey("backend")) {
            backend = properties.get("backend");
        } else {
            backend = "gremlin";
        }

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

        if (backend.equalsIgnoreCase("gremlinkafka")) {
            String kafka_config;
            if (properties.containsKey( "kafka_config" )) {
                kafka_config = properties.get( "kafka_config" );
            } else {
                kafka_config = "producer.properties";
            }

            setUpKafka( kafka_config );
            updateHandler = new KafkaUpdateHandler(producer, KAFKA_TOPIC);
        } else {
            updateHandler = new GremlinUpdateHandler(remoteClient);
        }
    }

    /**
     * Cluster/Client is configured through constructor. Just a utility method to retrieve client reference
     * @return Client for Remote Gremlin Server
     */
    public Client getClient() {
        return remoteClient;
    }

    public UpdateHandler getUpdateHandler() {
        return updateHandler;
    }

    @Override
    public void close() throws IOException {
        remoteClient.close();
        cluster.close();
        if (producer != null) {
            producer.close();
        }
    }
}
