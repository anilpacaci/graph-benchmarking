package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GremlinKafkaConsumer {
    public static String KAFKA_TOPIC = "ldbc_updates";
    private static String CONFIG_FILE = "consumer.properties";

    public static void main(String[] args) {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = GremlinKafkaConsumer.class.getClassLoader().getResourceAsStream( CONFIG_FILE );
            if (input == null) {
                System.out.println( "Sorry, unable to find " + CONFIG_FILE );
                return;
            }
            //load a properties file from class path, inside static method
            prop.load( input );
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
        KafkaConsumer<String, GremlinStatement> consumer = new KafkaConsumer<String, GremlinStatement>( prop );
        consumer.subscribe( Collections.singletonList( KAFKA_TOPIC ) );
        Map<String, String> props = new HashMap();
        props.put("locator", "localhost");
        GremlinDbConnectionState connection = new GremlinDbConnectionState(props);
        while (true) {
            ConsumerRecords<String, GremlinStatement> records = consumer.poll(100);
            for (ConsumerRecord<String, GremlinStatement> record : records) {
                GremlinStatement stmt = record.value();
                connection.getClient().submit(stmt.getStatement(), stmt.getParams());
            }
        }
    }
}
