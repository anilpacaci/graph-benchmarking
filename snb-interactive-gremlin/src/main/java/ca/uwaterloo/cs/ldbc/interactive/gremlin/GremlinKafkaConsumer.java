package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GremlinKafkaConsumer {
    public static String KAFKA_TOPIC = "ldbc_updates";

    final static Logger logger = LoggerFactory.getLogger(GremlinKafkaConsumer.class);


    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream input = null;
        String gremlin_config = args[0];
        String kafka_config = args[1];
        try {
            input = new FileInputStream(kafka_config);
            if (input == null) {
                System.out.println( "Sorry, unable to find " + kafka_config );
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

        Client client = Cluster.open(gremlin_config).connect();


        while (true) {
            ConsumerRecords<String, GremlinStatement> records = consumer.poll(0);
            for (ConsumerRecord<String, GremlinStatement> record : records) {
                GremlinStatement stmt = record.value();
                client.submit(stmt.getStatement(), stmt.getParams()).all().get();
                logger.info("Script submitted: {}", stmt.getStatement());
            }
        }
    }
}
