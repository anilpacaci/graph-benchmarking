package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LdbcComplexQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcQuery5 ldbcQuery5, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        //     * Description: Given a start Person,
        //     find the Forums which that Person's friends and friends of friends (excluding start Person)
        //     became Members of after a given date.
        //     For each forum find the number of Posts that were created by any of these Persons.
        //     For each Forum and consider only those Persons which joined that particular Forum after the given date.
        //     * Parameters: Person.id ID
        //     date Date
        //     * Results:
        //     Forum.title String
        //     count 32-bit Integer // number of Posts made in Forum that were created by friends
        //     * Sort:
        //     1st count (descending)
        //         2nd Forum.id (ascending)
        //     * Limit: 20

        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery5.personId() ) );
        params.put( "person_label", Entity.PERSON.getName() );
        params.put( "min_date", ldbcQuery5.minDate().getTime() );
        params.put( "result_limit", ldbcQuery5.limit() );

        // String statement = "g.V().has(person_label, 'iid', person_id).repeat(out('knows')).times(2).emit().dedup().aggregate('member')" +
        //         ".inE('hasMember').has('joinDate',gte(min_date)).outV().as('forum_name')" +
        //         ".out('containerOf').as('post').out('hasCreator').where(within('member')).select('post')" +
        //         ".groupCount().by(__.in('containerOf'))" +
        //         ".order(local).by(value, decr)" +
        //         //".order(local).by(key, incr)" +
        //         ".limit(local, 20);";
        String statement = "g.V().has(person_label, 'iid', person_id)." +
                "repeat(out('knows').simplePath()).times(2).dedup().aggregate('member')." +
                "inE('hasMember').has('joinDate',gte(min_date)).outV().as('forum_name')." +
                "out('containerOf').as('post').out('hasCreator').where(within('member')).select('post')." +
                "groupCount().by(__.in('containerOf'))." +
                "order(local).by(values, decr)." +
                "limit(local, result_limit)";
        /*
        g.V().has('person', 'iid', 'person:2202').
        repeat(out('knows').simplePath()).times(2).dedup().aggregate('member').
        inE('hasMember').has('joinDate',gte(1289520000000)).outV().as('forum_name').
        out('containerOf').as('post').out('hasCreator').where(within('member')).
        select('post').
        groupCount().by(__.in('containerOf')).
        order(local).by(values, decr).
        limit(local, 20)

         2202,
      ,
      20


         */
        List<Result> results;
        try
        {
            results = client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        HashMap<Vertex, Long> resultMap = results.get( 0 ).get( HashMap.class );

        List<LdbcQuery5Result> resultList = new ArrayList<>();
        List<Map.Entry<Vertex, Long>> sortedList = resultMap.entrySet().stream().collect( Collectors.toList() );
        sortedList.sort( Comparator.comparing( r2 -> r2.getKey().<Long>property( "iid_long" ).value() ) );
        for ( Map.Entry<Vertex, Long> r : sortedList )
        {
            Vertex forum = r.getKey();
            String forum_name = forum.<String>property( "title" ).value();
            Long count = r.getValue();

            LdbcQuery5Result ldbcQuery5Result = new LdbcQuery5Result(
                    forum_name,
                    count.intValue()
            );

            resultList.add( ldbcQuery5Result );
        }

        resultReporter.report( resultList.size(), resultList, ldbcQuery5 );
    }
}
