package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LdbcKafkaProducer {
    public static String KAFKA_TOPIC = "ldbc_updates";
    private static String CONFIG_FILE = "producer.properties";
    private static KafkaProducer<String, GremlinStatement> producer = null;

    public static KafkaProducer<String, GremlinStatement> createProducer() {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = GremlinDbConnectionState.class.getClassLoader().getResourceAsStream( CONFIG_FILE );
            if (input == null) {
                System.out.println( "Sorry, unable to find " + CONFIG_FILE );
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
        return new KafkaProducer<String, GremlinStatement>(prop);
    }
}
