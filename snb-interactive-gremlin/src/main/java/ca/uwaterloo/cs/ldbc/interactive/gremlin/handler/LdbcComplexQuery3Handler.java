package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcQuery3 ldbcQuery3, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery3.personId() ) );
        params.put( "person_label", Entity.PERSON.getName() );
        params.put( "countryX", ldbcQuery3.countryXName() );
        params.put( "countryY", ldbcQuery3.countryYName() );
        Date start = ldbcQuery3.startDate();
        Long end = new DateTime( start ).plusDays( ldbcQuery3.durationDays() ).toDate().getTime();
        params.put( "start_date", start.getTime() );
        params.put( "end_date", end );
        params.put( "result_limit", ldbcQuery3.limit() );

        // String statement = "g.V().has(person_label, 'iid', person_id)" +
        //         ".repeat(out('knows')).times(2).emit().as('person')" +
        //         ".where(out('isLocatedIn').out('isPartOf').has('name', neq(countryX))" +
        //         ".and().out('isLocatedIn').out('isPartOf').has('name', neq(countryY)))" +
        //         ".in('hasCreator')" +
        //         ".where(out('isLocatedIn').has('name', countryX)" +
        //         ".or().out('isLocatedIn').has('name', countryY))" +
        //         ".has('creationDate', inside(start_date, end_date)).as('message')" +
        //         ".out('hasCreator').order().by('iid_long')" +
        //         ".select('message').group().by(out('hasCreator'))" +
        //         ".by(groupCount().by(out('isLocatedIn')).values('name')))" +
        //         ".limit(local, result_limit)";

        String statement = " g.V().has(person_label, 'iid', person_id)." +
                " repeat(out('knows').simplePath()).times(2).dedup().as('person')." +
                " values('iid_long').as('pid')." +
                " select('person').where(out('isLocatedIn').out('isPartOf').has('name', neq(countryX))." +
                " and().out('isLocatedIn').out('isPartOf').has('name', neq(countryY)))." +
                " match(" +
                "         __.as('p').in('hasCreator').where(out('isLocatedIn').has('name', countryX)).count().as('countx')," +
                "         __.as('p').in('hasCreator').where(out('isLocatedIn').has('name', countryY)).count().as('county')" +
                " ).select('pid', 'person', 'countx', 'county')." +
                " sort{-it.get('countx')}." +
                " sort{it.get('pid')};";
        /*
        g= Neo4jGraph.open('/hdd1/ldbc/datasets/neo4j/validation/').traversal()
        g.V().has('person', 'iid', 'person:234').
        repeat(out('knows').simplePath()).times(2).dedup().as('person').
        values('iid_long').as('pid').
        select('person').where(out('isLocatedIn').out('isPartOf').has('name', neq('Canada')).
        and().out('isLocatedIn').out('isPartOf').has('name', neq('United States'))).
        match(
           __.as('p').in('hasCreator').where(out('isLocatedIn').has('name', 'Canada')).count().as('countx'),
           __.as('p').in('hasCreator').where(out('isLocatedIn').has('name', 'United States')).count().as('county')
        ).select('pid', 'person', 'countx', 'county').
        sort{-it.get('countx')}.
        sort{it.get('pid')}.
        collect().subList(0, 10)
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

        List<LdbcQuery3Result> resultList = new ArrayList<>();
        for ( Result r : results.subList( 0,
                results.size() > ldbcQuery3.limit() ? ldbcQuery3.limit() : results.size() ) )
        {
            HashMap map = r.get( HashMap.class );
            Vertex person = (Vertex) map.get( "person" );
            long countx = (long) map.get( "countx" );
            long county = (long) map.get( "county" );

            LdbcQuery3Result ldbcQuery3Result = new LdbcQuery3Result(
                    GremlinUtils.getSNBId( person ),
                    person.<String>property( "firstName" ).value(),
                    person.<String>property( "lastName" ).value(),
                    countx,
                    county,
                    countx + county
            );

            resultList.add( ldbcQuery3Result );
        }

        resultReporter.report( resultList.size(), resultList, ldbcQuery3 );
    }
}
