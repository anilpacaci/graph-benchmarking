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

        params.put("vlabel", Entity.PERSON.getName());
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate1AddPerson.personId()));
        params.put("person_id_long", ldbcUpdate1AddPerson.personId());
        params.put("located_in", GremlinUtils.makeIid(Entity.PLACE, ldbcUpdate1AddPerson.cityId()));
        params.put("firstName", ldbcUpdate1AddPerson.personFirstName());
        params.put("lastName", ldbcUpdate1AddPerson.personLastName());
        params.put("gender", ldbcUpdate1AddPerson.gender());
        params.put("birthday", ldbcUpdate1AddPerson.birthday().getTime());
        params.put("creation_date", ldbcUpdate1AddPerson.creationDate().getTime());
        params.put("location_ip", ldbcUpdate1AddPerson.locationIp());
        params.put("browser_used", ldbcUpdate1AddPerson.browserUsed());

        params.put("languages", ldbcUpdate1AddPerson.languages());
        params.put("emails", ldbcUpdate1AddPerson.emails());
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate1AddPerson.tagIds()));

        params.put("place_label", Entity.PLACE.getName());
        params.put("tag_label", Entity.TAG.getName());

        String statement = "person = g.addV(label, vlabel)" +
            ".property('iid', person_id)" +
            ".property('iid_long', person_id_long)" +
            ".property('firstName', firstName)" +
            ".property('lastName', lastName)" +
            ".property('gender', gender) " +
            ".property('birthday', birthday) " +
            ".property('creationDate', creation_date)" +
            ".property('locationIP', location_ip)" +
            ".property('browserUsed', browser_used).next();" +
            "city = g.V().has(place_label, 'iid', located_in).next();" +
            "person.addEdge('isLocatedIn', city);" +
            "languages.forEach{l ->  person.property('language', l); };" +
            "emails.forEach{l ->  person.property('email', l); };" +
            "tag_ids.forEach{t ->  tag = g.V().has(tag_label, 'iid', t).next(); person.addEdge('hasInterest', tag); }";

        String uni_statement = ldbcUpdate1AddPerson.studyAt().stream()
            .map(org -> {
                Object iid = GremlinUtils.makeIid(Entity.ORGANISATION, org.organizationId());
                return String.format("v = g.V().has('%s', 'iid', '%s').next();" +
                    "e = person.addEdge('studyAt', v); e.property('classYear', '%s');", Entity.ORGANISATION.getName(), iid, String.valueOf(org.year()));
            })
            .collect( Collectors.joining("\n"));
        String company_statement = ldbcUpdate1AddPerson.workAt().stream()
            .map(org -> {
                Object iid = GremlinUtils.makeIid(Entity.ORGANISATION, org.organizationId());
                return String.format("v = g.V().has('%s', 'iid', '%s').next();" +
                    "e = person.addEdge('workAt', v); e.property('workFrom', '%s');", Entity.ORGANISATION.getName(), iid, String.valueOf(org.year()));
            })
            .collect(Collectors.joining("\n"));

        statement = String.join("\n", statement, uni_statement, company_statement);

        try {
            client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate1AddPerson);

    }
}
