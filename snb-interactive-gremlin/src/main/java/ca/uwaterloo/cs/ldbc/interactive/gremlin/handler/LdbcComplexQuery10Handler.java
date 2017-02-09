package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery10Handler implements OperationHandler<LdbcQuery10, DbConnectionState> {
  @Override
  public void executeOperation( LdbcQuery10 ldbcQuery10, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException {

    Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
    Map<String, Object> params = new HashMap<>();
    params.put( "person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery10.personId() ) );
    params.put( "person_label", Entity.PERSON.getName() );
    params.put( "result_limit", ldbcQuery10.limit() );
    params.put( "month", ldbcQuery10.month() );

    String statement =
            "g.V().has(person_label, 'iid', person_id).as('startPerson').                    "
                    +"   out('hasInterest').aggregate('persontags').                                 "
                    +"   select('startPerson').                                                      "
                    +"   repeat(out('knows')).times(2).dedup().                                      "
                    +"   filter{ it -> ts = it.get().value('birthday');                              "
                    +"       Calendar cal = Calendar.getInstance();                                  "
                    +"       Calendar lowercal = Calendar.getInstance();                             "
                    +"       Calendar highercal = Calendar.getInstance();                            "
                    +"       cal.setTime(new java.util.Date(Long.parseLong(ts)));                    "
                    +"       int day = cal.get(Calendar.DAY_OF_MONTH);                               "
                    +"       int month = cal.get(Calendar.MONTH);                                    "
                    +"       int year = cal.get(Calendar.YEAR);                                      "
                    +"       month = day < 21 ? month -1 : month;                                    "
                    +"       lowercal.set(year, month, 21);                                          "
                    +"       highercal.set(year, month+1, 22);                                       "
                    +"       return lowercal.compareTo(cal) <= 0 && highercal.compareTo(cal) > 0;    "
                    +"   }.as('fof').match(                                                                                                             "
                    +"      __.as('p').in('hasCreator').hasLabel('post').out('hasTag').where(within('persontags')).count().fold(2, mult).as('common2'), "
                    +"      __.as('p').in('hasCreator').hasLabel('post').out('hasTag').count().fold(-1, mult).as('totaln'),                             "
                    +"        __.as('common2').map(union(identity(), select('totaln')).sum()).as('similarity')                                          "
                    +"      ).select('p').out('isLocatedIn').as('city').select('p', 'city', 'similarity').sort{-it.get('similarity')}                                                                                               ";

    List<Result> results = null;

    try {
      results = client.submit( statement, params ).all().get();

    } catch (InterruptedException | ExecutionException e) {
      throw new DbException( "Remote execution failed", e );
    }


    List<LdbcQuery10Result> resultList = new ArrayList<>();
    for (Result r : results) {
      HashMap map = r.get( HashMap.class );
      Vertex person = (Vertex) map.get( "p" );
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

    resultReporter.report( resultList.size(), resultList.subList(0, ldbcQuery10.limit()), ldbcQuery10 );
  }
}
