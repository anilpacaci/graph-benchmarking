package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery1Handler implements OperationHandler<LdbcQuery1, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery1 ldbcQuery1, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();

        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery1.personId()));
        params.put("person_label", Entity.PERSON.getName());
        params.put("firstName", ldbcQuery1.firstName());
        params.put("result_limit", ldbcQuery1.limit());

        String statement = "matchList = []; distList = [] ; g.withSideEffect('x', matchList).withSideEffect('d', distList).V().has(person_label, 'iid', person_id).aggregate('done')" +
                ".out('knows').where(without('done')).dedup().fold()" +
                ".sideEffect(unfold().has('firstName', firstName).order().by('lastName', incr).by(id(), incr).limit(result_limit).as('person').select('x').by(count(Scope.local)).is(lt(result_limit)).store('x').by(select('person')))" +
                ".filter(select('x').count(Scope.local).is(lt(result_limit)).store('d')).unfold().aggregate('done').out('knows').where(without('done')).dedup().fold()" +
                ".sideEffect(unfold().has('firstName', firstName).order().by('lastName', incr).by(id(), incr).limit(result_limit).as('person').select('x').by(count(Scope.local)).is(lt(result_limit)).store('x').by(select('person')))" +
                ".filter(select('x').count(Scope.local).is(lt(result_limit)).store('d')).unfold().aggregate('done').out('knows').where(without('done')).dedup().fold()" +
                ".sideEffect(unfold().has('firstName', firstName).order().by('lastName', incr).by(id(), incr).limit(result_limit).as('person').select('x').by(count(Scope.local)).is(lt(result_limit)).store('x').by(select('person')))" +
                ".select('x').count(Scope.local).store('d').iterate(); " +
                "matchListIds = matchList.collect({i -> i.property('iid').value()}); " +
                "propertiesMap = [:]; " +
                "g.V().has(person_label, 'iid', within(matchListIds)).as('person').<List<String>>valueMap().as('props').select('person', 'props')" +
                ".forEachRemaining({map -> propertiesMap.put((Vertex) map.get('person'),(Map<String, List<String>>) map.get('props')); }); " +
                "placeNameMap = [:]; " +
                "g.V().has(person_label, 'iid', within(matchListIds)).as('person').out('isLocatedIn').<String>values('name').as('placeName').select('person', 'placeName')" +
                ".forEachRemaining({map -> placeNameMap.put((Vertex) map.get('person'), (String) map.get('placeName')); }); " +
                "universityInfoMap = [:]; " +
                "g.V().has(person_label, 'iid', within(matchListIds)).as('person') .outE('studyAt').as('classYear') .inV().as('universityName') .out('isLocatedIn').as('cityName') .select('person', 'universityName', 'classYear', 'cityName') " +
                ".by().by('name').by('classYear').by('name') .forEachRemaining({ map -> v = map.get('person'); " +
                "tuple = []; " +
                "tuple.add(map.get('universityName')); " +
                "tuple.add(Integer.decode((String) map.get('classYear'))); " +
                "tuple.add(map.get('cityName')); " +
                "if (universityInfoMap.containsKey(v)) { universityInfoMap.get(v).add(tuple); } else { tupleList = []; tupleList.add(tuple); universityInfoMap.put(v, tupleList); } }); " +
                "companyInfoMap =[:] ; " +
                "g.V().has(person_label, 'iid', within(matchListIds)).as('person') .outE('workAt').as('workFrom') .inV().as('companyName') .out('isLocatedIn').as('cityName') .select('person', 'companyName', 'workFrom', 'cityName') " +
                ".by().by('name').by('workFrom').by('name') .forEachRemaining({ map ->  v = map.get('person'); " +
                "tuple = []; " +
                "tuple.add(map.get('companyName')); " +
                "tuple.add(Integer.decode((String) map.get('workFrom'))); " +
                "tuple.add(map.get('cityName')); " +
                "if (companyInfoMap.containsKey(v)) { companyInfoMap.get(v).add(tuple); } else { tupleList = []; tupleList.add(tuple); companyInfoMap.put(v, tupleList); } }); " +
                "['matchList':matchList, 'distList':distList, 'propertiesMap':propertiesMap, 'placeNameMap':placeNameMap, 'universityInfoMap':universityInfoMap, 'companyInfoMap':companyInfoMap]";

        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery1Result> result = new ArrayList<>();

        List<Vertex> matchList = (List<Vertex>) results.get(0).get(AbstractMap.SimpleEntry.class).getValue();
        List<Long> distList = (List<Long>) results.get(1).get(AbstractMap.SimpleEntry.class).getValue();
        Map<Vertex, Map<String, List<String>>> propertiesMap = (Map<Vertex, Map<String, List<String>>>) results.get(2).get(AbstractMap.SimpleEntry.class).getValue();
        Map<Vertex, String> placeNameMap = (Map<Vertex, String>) results.get(3).get(AbstractMap.SimpleEntry.class).getValue();
        Map<Vertex, List<List<Object>>> universityInfoMap = (Map<Vertex, List<List<Object>>>) results.get(4).get(AbstractMap.SimpleEntry.class).getValue();
        Map<Vertex, List<List<Object>>> companyInfoMap = (Map<Vertex, List<List<Object>>>) results.get(5).get(AbstractMap.SimpleEntry.class).getValue();

        for (int i = 0; i < matchList.size(); i++) {
            Vertex match = matchList.get(i);
            int distance = (i < distList.get(0)) ? 1
                    : (i < distList.get(1)) ? 2 : 3;
            Map<String, List<String>> properties = propertiesMap.get(match);
            List<String> emails = properties.get("email");
            if (emails == null) {
                emails = new ArrayList<>();
            }
            List<String> languages = properties.get("language");
            if (languages == null) {
                languages = new ArrayList<>();
            }
            String placeName = placeNameMap.get(match);
            List<List<Object>> universityInfo = universityInfoMap.get(match);
            if (universityInfo == null) {
                universityInfo = new ArrayList<>();
            }
            List<List<Object>> companyInfo = companyInfoMap.get(match);
            if (companyInfo == null) {
                companyInfo = new ArrayList<>();
            }
            result.add(new LdbcQuery1Result(
                    GremlinUtils.getSNBId(match),
                    properties.get("lastName").get(0),
                    distance,
                    Long.decode(properties.get("birthday").get(0)),
                    Long.decode(properties.get("creationDate").get(0)),
                    properties.get("gender").get(0),
                    properties.get("browserUsed").get(0),
                    properties.get("locationIP").get(0),
                    emails,
                    languages,
                    placeName,
                    universityInfo,
                    companyInfo));
        }

        resultReporter.report(result.size(), result, ldbcQuery1);
    }
}
