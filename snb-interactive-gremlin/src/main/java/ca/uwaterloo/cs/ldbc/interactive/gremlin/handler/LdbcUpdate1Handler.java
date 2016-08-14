package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LdbcUpdate1Handler implements OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate1AddPerson ldbcUpdate1AddPerson, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();

        params.put("located_in", GremlinUtils.makeIid(Entity.PLACE, ldbcUpdate1AddPerson.cityId()));
        params.put("firstName", ldbcUpdate1AddPerson.personFirstName());
        params.put("lastName", ldbcUpdate1AddPerson.personLastName());
        params.put("gender", ldbcUpdate1AddPerson.gender());
        params.put("birthday", String.valueOf(ldbcUpdate1AddPerson.birthday().getTime()));
        params.put("creation_date", String.valueOf(ldbcUpdate1AddPerson.creationDate().getTime()));
        params.put("location_ip", ldbcUpdate1AddPerson.locationIp());
        params.put("browser_used", ldbcUpdate1AddPerson.browserUsed());

        params.put("languages", ldbcUpdate1AddPerson.languages());
        params.put("emails", ldbcUpdate1AddPerson.emails());
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate1AddPerson.tagIds()));

        String statement = "person = g.addV()" +
            ".property('iid', person_id')" +
            ".property('located_in', located_in)" +
            ".property('firstName', firstName)" +
            ".property('lastName', lastName)" +
            ".property('gender', gender); " +
            ".property('birthday', birthday); " +
            ".property('creation_date', creation_date)" +
            ".property('location_ip', location_ip)" +
            ".property('browser_used', browser_used);" +
            "city = g.V().has('iid', located_in).next();" +
            "person.outE('isLocatedIn', city);" +
            "languages.forEach{l ->  person.property('language', l); };" +
            "emails.forEach{l ->  person.property('email', l); };" +
            "tags_ids.forEach{t ->  tag = g.V().has('iid', t).next(); post.addEdge('hasTag', tag); }";

        String uni_statement = ldbcUpdate1AddPerson.studyAt().stream()
            .map(org -> {
                Object iid = GremlinUtils.makeIid(Entity.ORGANISATION, org.organizationId());
                return String.format("v = g.addVertex(); v.property('iid', %s);" +
                    "e = person.addEdge('studyAt', v); e.property('classYear', %s);", iid, String.valueOf(org.year()));
            })
            .collect( Collectors.joining("\n"));
        String company_statement = ldbcUpdate1AddPerson.workAt().stream()
            .map(org -> {
                Object iid = GremlinUtils.makeIid(Entity.ORGANISATION, org.organizationId());
                return String.format("v = g.addVertex(); v.property('iid', %s);" +
                    "e = person.addEdge('workAt', v); e.property('workFrom', %s);", iid, String.valueOf(org.year()));
            })
            .collect(Collectors.joining("\n"));

        try {
            client.submit( String.join("\n",
                statement,
                uni_statement,
                company_statement),
                params)
                .all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate1AddPerson);

    }
}
