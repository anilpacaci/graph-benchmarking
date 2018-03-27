package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import com.ldbc.driver.DbException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 5/30/17.
 */
public class GremlinDriverTest {

    private static GremlinDbConnectionState connectionState;

    @BeforeClass
    public static void initializeResources() {
        Map<String, String> conf = new HashMap<>();
        conf.put("locator", "src/test/resources/remote-objects.yaml");

        connectionState = new GremlinDbConnectionState(conf);
    }

    @Test
    public void OneHopeFriendship() throws DbException {
        Client client = ((GremlinDbConnectionState) connectionState).getClient();

        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, 933));
        params.put("person_label", Entity.PERSON.getName());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".out('knows').count()";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        int count = results.get(0).getInt();

        Assert.assertTrue( count > 0 );
    }

    @AfterClass
    public static void releaseResources() throws IOException {
        connectionState.close();
    }
}
