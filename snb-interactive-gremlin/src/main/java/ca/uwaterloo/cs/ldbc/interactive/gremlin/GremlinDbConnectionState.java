package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import com.ldbc.driver.DbConnectionState;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by apacaci on 7/14/16.
 */
public class GremlinDbConnectionState extends DbConnectionState {

    final static Logger logger =
            LoggerFactory.getLogger(GremlinDbConnectionState.class);

    private Cluster cluster;
    private Client remoteClient;

    public GremlinDbConnectionState(Map<String, String> properties) {
        String remoteObjectFile;
        if (properties.containsKey("locator")) {
            remoteObjectFile = properties.get("locator");
        } else {
            remoteObjectFile = "127.0.0.1";
        }

        try {
            cluster = Cluster.open(remoteObjectFile);
            remoteClient = cluster.connect();
        } catch (Exception e) {
            logger.error("Connection to remote Gremlin Server could NOT obtained");
        }
    }

    /**
     * Cluster/Client is configured through constructor. Just a utility method to retrieve client reference
     * @return Client for Remote Gremlin Server
     */
    public Client getClient() {
        return this.remoteClient;
    }

    @Override
    public void close() throws IOException {
        remoteClient.close();
        cluster.close();
    }
}
