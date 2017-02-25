package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created by anilpacaci on 2016-07-23.
 */
public class LdbcComplexQuery12Handler implements OperationHandler<LdbcQuery12, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcQuery12 ldbcQuery12, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery12.personId() ) );
        params.put( "person_label", Entity.PERSON.getName() );
        params.put( "tagclass", ldbcQuery12.tagClassName() );
        params.put( "result_limit", ldbcQuery12.limit() );

        //String statement = "g.V().has(person_label, 'iid', person_id)" +
        //        ".out('knows').limit(result_limit).as('friends')" +
        //        ".in('hasCreator').where(out('replyOf').hasLabel('post')" +
        //        ".out('hasTag').repeat(out('hasType')).until(has('name', tagclass))).as('messages')" +
        //        ".out('hasTag').values('name').as('tags')" +
        //        ".select('friends', 'messages', 'tags')";
        String statement = "g.V().has(person_label, 'iid', person_id)." +
                "out('knows').as('friends').values('iid_long').as('pid')." +
                "select('friends').match(" +
                "__.as('f').in('hasCreator').hasLabel('comment')." +
                "        where(out('replyOf').hasLabel('post').out('hasTag')." +
                "        repeat(out('hasType')).until(has('name', 'Person'))).fold().as('comments')," +
                "        __.as('comments').unfold().out('hasTag').values('name').fold().as('tagnames')," +
                "        __.as('comments').unfold().count().as('count')" +
                ").select('comments').out('hasTag').values('name').as('tagnames')." +
                "select('pid', 'friends', 'count', 'tagnames')." +
                "sort{-it.get('count')}." +
                "sort{it.get('pid')}." +
                "collect()." +
                "subList(0, result_limit)";
        /*
        g= Neo4jGraph.open('/hdd1/ldbc/datasets/neo4j/validation/').traversal()
        g.V().has('person', 'iid', 'person:234').
        out('knows').as('friends').values('iid_long').as('pid').
        select('friends').match(
        __.as('f').in('hasCreator').hasLabel('comment').
            where(out('replyOf').hasLabel('post').out('hasTag').
            repeat(out('hasType')).until(has('name', 'Person'))).fold().as('comments'),
        __.as('comments').unfold().out('hasTag').values('name').fold().as('tagnames'),
        __.as('comments').unfold().count().as('count')
        ).select('pid', 'friends', 'count', 'tagnames').
        sort{-it.get('count')}.
        sort{it.get('pid')}.
        collect().
        subList(0, 20)

         */

        List<Result> results = null;
        try
        {
            results = client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        ArrayList<LdbcQuery12Result> ldbcQuery12Results = new ArrayList<>();
        for ( Result r : results )
        {
            HashMap map = r.get( HashMap.class );
            Vertex person = (Vertex) map.get( "friends" );
            Long count = (Long) map.get( "count" );
            List<Object> tags = (List<Object>) map.get( "tags" );
            List<String> tagList = tags.stream().map( Object::toString ).collect( Collectors.toList() );

            LdbcQuery12Result ldbcQuery12Result = new LdbcQuery12Result( GremlinUtils.getSNBId( person ), person.<String>property( "firstName" ).value(),
                    person.<String>property( "lastName" ).value(),
                    tagList,
                    count.intValue() );
            ldbcQuery12Results.add( ldbcQuery12Result );
        }

        resultReporter.report( ldbcQuery12Results.size(), ldbcQuery12Results, ldbcQuery12 );
    }
}
