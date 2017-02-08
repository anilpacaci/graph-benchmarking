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
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      "Calendar cal = Calendar.getInstance();                                                              "
        + "Calendar lowercal = Calendar.getInstance();                                                         "
        + "Calendar highercal = Calendar.getInstance();                                                        "
        + "g.V().has(person_label, 'iid', person_id).as('startPerson')                                                     "
        + "   .out('hasInterest').aggregate('persontags')                                                      "
        + "   .select('startPerson')                                                                           "
        + "   .repeat(out()).times(2).path().by('knows')                                                       "
        + "   .filter{ it => {                                                                                 "
        + "       ts = it.get().value('birthDay')                                                              "
        + "       cal.setTime(new java.util.Date((long)ts**1000));                                             "
        + "       int day = cal.get(Calendar.DAY_OF_MONTH);                                                    "
        + "       int month = cal.get(Calendar.MONTH);                                                         "
        + "       int year = cal.get(Calendar.YEAR);                                                           "
        + "                                                                                                    "
        + "       if (day < 21) {                                                                              "
        + "         lowercal.set(year, month-1, 21)                                                            "
        + "         highercal.set(year, month, 22)                                                             "
        + "       } else {                                                                                     "
        + "         lowercal.set(year, month, 21)                                                              "
        + "         highercal.set(year, month+1, 22)                                                           "
        + "       }                                                                                            "
        + "                                                                                                    "
        + "       return lowercal.compareTo(cal) <= 0 && highercal.compareTo(cal) > 0;                         "
        + "     }                                                                                              "
        + "   }                                                                                                "
        + "   .has('birthDay',inside((year, month, 21),(year, month+1, 22))).as('fof')                         "
        + "   .map(                                                                                            "
        + "     in('hasCreator').hasLabel('post').where(out('hasTag')).match(                                  "
        + "        __.as('tag').where(within('persontags')).count().as('commonCount')                          "
        + "        __.as('tag').where(without('persontags')).count().as('uncommonCount')                       "
        + "        __.as('uncommonCount').map(union(identity(), constant(-1))).fold(1, mult)                   "
        + "        __.as('commonCount').map(union(identity(), select('uncommonCount')).sum()).as('similarity') "
        + "        )                                                                                           "
        + "     )                                                                                              "
        + "   ).select('fof', 'similarity').order().by('similarity').limit(result_limit)                                 ";

    List<Result> results = null;
    try {
      results = client.submit( statement, params ).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException( "Remote execution failed", e );
    }


    List<LdbcQuery10Result> resultList = new ArrayList<>();
    for (Result r : results) {
      HashMap map = r.get( HashMap.class );
      Vertex person = (Vertex) map.get( "person" );
      int similarity = (int) map.get( "similarity" );

      LdbcQuery10Result ldbcQuery10Result = new LdbcQuery10Result( GremlinUtils.getSNBId( person ),
        person.<String>property( "firstName" ).value(),
        person.<String>property( "lastName" ).value(),
        similarity,
        person.<String>property( "gender" ).value(),
        person.<String>property( "cityName" ).value()
      );

      resultList.add( ldbcQuery10Result );
    }
    resultReporter.report( resultList.size(), resultList, ldbcQuery10 );
  }
}
