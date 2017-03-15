package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 * <p>
 * Based on reference Titan LDBC SNB Interactive on offficial LDBC Github Page
 * <a href="https://github.com/ldbc/ldbc_snb_implementations/blob/master/interactive/titan/titanFTM_driver/src/main/java/hpl/alp2/titan/drivers/interactive/LdbcQuery7Handler.java">ComplexQuery7 Native Titan</a>/>
 */
public class LdbcComplexQuery7Handler implements OperationHandler<LdbcQuery7, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcQuery7 ldbcQuery7, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery7.personId() ) );
        params.put( "person_label", Entity.PERSON.getName() );
        params.put( "result_limit", ldbcQuery7.limit() );

        List<Result> authorKnowsResults = null;
        try
        {
            authorKnowsResults = client.submit( " g.V().has('iid', person_id).out('knows')", params ).all().get();

        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        List<Vertex> authorKnows = new ArrayList<>();
        authorKnowsResults.forEach( res ->
        {
            authorKnows.add( res.getVertex() );
        } );

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".in('hasCreator').as('post')" +
                ".inE('likes').as('like')" +
                ".outV().order().by('iid_long').as('liker')" +
                ".select('like').order().by('creationDate', decr)" +
                ".limit(result_limit)" +
                ".select('post', 'like', 'liker').by().by('creationDate').by()";

        List<Result> results = null;
        try
        {
            results = client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        List<LdbcQuery7Result> result = new ArrayList<>();

        for ( Result r : results )
        {
            HashMap map = r.get( HashMap.class );
            Long likeDate = (Long) map.get( "like" );
            Vertex liker = (Vertex) map.get( "liker" );
            Vertex post = (Vertex) map.get( "post" );

            boolean notKnows = !authorKnows.contains( liker );

            long id = GremlinUtils.getSNBId( liker );
            String firstName = liker.<String>property( "firstName" ).value();
            String lastName = liker.<String>property( "lastName" ).value();
            long postDate = post.<Long>property( "creationDate" ).value();
            long postID = GremlinUtils.getSNBId( post );
            String content = post.<String>property( "content" ).value();
            if ( content.isEmpty() )
            {
                content = post.<String>property( "imageFile" ).value();
            }
            int latency = (int) ((likeDate - postDate) / 60000);

            result.add( new LdbcQuery7Result( id, firstName, lastName, likeDate, postID, content, latency, notKnows ) );

        }

        resultReporter.report( result.size(), result, ldbcQuery7 );
    }
}
