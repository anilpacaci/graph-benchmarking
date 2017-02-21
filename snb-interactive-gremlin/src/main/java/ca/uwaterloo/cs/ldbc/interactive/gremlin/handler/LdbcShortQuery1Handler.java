package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/14/16.
 */
public class LdbcShortQuery1Handler implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcShortQuery1PersonProfile.personId()));
        params.put("person_label", Entity.PERSON.getName());

        String statement = "g.V().has(person_label, 'iid', person_id).as('person').outE('isLocatedIn').inV().as('place').select('person', 'place')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Result r = results.get(0); // ShortQuery1 should return a single result

        HashMap map = r.get(HashMap.class);
        Vertex person = (Vertex) r.get(HashMap.class).get("person");
        Vertex place = (Vertex) r.get(HashMap.class).get("place");

        Iterator<VertexProperty<Object>> props = person.properties();
        Map<String, Object> propertyMap = new HashMap<>();
        props.forEachRemaining((prop) -> {
            propertyMap.put(prop.key(), prop.value());
        });

        long placeId = GremlinUtils.getSNBId(place);

        LdbcShortQuery1PersonProfileResult res =
                new LdbcShortQuery1PersonProfileResult(
                        (String) propertyMap.get("firstName"),
                        (String) propertyMap.get("lastName"),
                        (Long) propertyMap.get("birthday"),
                        (String) propertyMap.get("locationIP"),
                        (String) propertyMap.get("browserUsed"),
                        placeId,
                        (String) propertyMap.get("gender"),
                        (Long) propertyMap.get("creationDate"));

        resultReporter.report(0, res, ldbcShortQuery1PersonProfile);
    }
}
