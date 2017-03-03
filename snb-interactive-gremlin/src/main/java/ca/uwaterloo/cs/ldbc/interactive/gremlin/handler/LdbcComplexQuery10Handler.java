package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery10Handler implements OperationHandler<LdbcQuery10, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcQuery10 ldbcQuery10, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {

        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery10.personId() ) );
        params.put( "person_label", Entity.PERSON.getName() );
        params.put( "result_limit", ldbcQuery10.limit() );
        params.put( "givenmonth", ldbcQuery10.month() );

        String statement = "g.V().has(person_label, 'iid', person_id).as('startPerson').aggregate('0')." +
                "out('hasInterest').aggregate('persontags')." +
                "select('startPerson').out('knows').aggregate('1')." +
                "out('knows').where(without('0')).where(without('1')).dedup()." +
                "filter{ it -> ts = it.get().value('birthday');" +
                "    Calendar cal = Calendar.getInstance();" +
                "    Calendar lowercal = Calendar.getInstance();" +
                "    Calendar highercal = Calendar.getInstance();" +
                "    cal.setTime(new java.util.Date(ts));" +
                "    int day = cal.get(Calendar.DAY_OF_MONTH);" +
                "    int month = cal.get(Calendar.MONTH) + 1;" +
                "    month = day < 21 ? month-1 : month;" +
                "    return month == givenmonth" +
                "}.as('fof').match(" +
                "   __.as('p').in('hasCreator').hasLabel('post').where(out('hasTag').where(within('persontags')).count().is(gt(0))).count().fold(2, mult).as('common2')," +
                "   __.as('p').in('hasCreator').hasLabel('post').count().fold(-1, mult).as('totaln')," +
                "   __.as('common2').map(union(identity(), select('totaln')).sum()).as('similarity')" +
                ").select('fof').out('isLocatedIn').as('city').select('fof').values('iid_long').as('pid')." +
                "order().by(select('similarity'), decr).by(select('pid'))." +
                "limit(result_limit)." +
                "select('pid', 'fof', 'city', 'similarity')";

        List<Result> results = null;

        try
        {
            results = client.submit( statement, params ).all().get();

        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }


        List<LdbcQuery10Result> resultList = new ArrayList<>();
        for ( Result r : results)
        {
            HashMap map = r.get( HashMap.class );
            Vertex person = (Vertex) map.get( "fof" );
            Vertex city = (Vertex) map.get( "city" );
            Long similarity = (Long) map.get( "similarity" );

            LdbcQuery10Result ldbcQuery10Result = new LdbcQuery10Result( GremlinUtils.getSNBId( person ),
                    person.<String>property( "firstName" ).value(),
                    person.<String>property( "lastName" ).value(),
                    similarity.intValue(),
                    person.<String>property( "gender" ).value(),
                    city.<String>property( "name" ).value()
            );

            resultList.add( ldbcQuery10Result );
        }

        resultReporter.report( resultList.size(), resultList, ldbcQuery10 );
    }
}
