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
        Map<String, Object> props = new HashMap<>();

        props.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate1AddPerson.personId()));
        params.put("located_in", GremlinUtils.makeIid(Entity.PLACE, ldbcUpdate1AddPerson.cityId()));
        props.put("firstName", ldbcUpdate1AddPerson.personFirstName());
        props.put("lastName", ldbcUpdate1AddPerson.personLastName());
        props.put("gender", ldbcUpdate1AddPerson.gender());
        props.put("birthday", String.valueOf(ldbcUpdate1AddPerson.birthday().getTime()));
        props.put("creation_date", String.valueOf(ldbcUpdate1AddPerson.creationDate().getTime()));
        props.put("location_ip", ldbcUpdate1AddPerson.locationIp());
        props.put("browser_used", ldbcUpdate1AddPerson.browserUsed());

        String statement = "person = g.addVertex(props);" +
            "city = g.V().has(iid, located_in).next();" +
            "person.outE('isLocatedIn', city);";

        String lang_statement = ldbcUpdate1AddPerson.languages()
            .stream()
            .map(l -> String.format("person.property('language', %s);", l))
            .collect(Collectors.joining("\n"));

        String email_statement = ldbcUpdate1AddPerson.emails()
            .stream()
            .map(e -> String.format("person.property('email', %s);", e))
            .collect(Collectors.joining("\n"));

        String tag_statement = ldbcUpdate1AddPerson.tagIds()
            .stream()
            .map(t -> String.format("tag = g.V().has('iid', %s);" +
                "tag.hasNext() && post.addEdge('hasInterest', tag);", t))
            .collect( Collectors.joining("\n"));

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
            .collect( Collectors.joining("\n"));
        params.put("props", props);

        try {
            client.submit( String.join("\n",
                statement,
                lang_statement,
                email_statement,
                tag_statement,
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
