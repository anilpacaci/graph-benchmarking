package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery14Handler implements OperationHandler<LdbcQuery14, DbConnectionState> {
  @Override
  public void executeOperation( LdbcQuery14 ldbcQuery14, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException {

    Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
    Map<String, Object> params = new HashMap<>();
    params.put( "person1_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery14.person1Id() ) );
    params.put( "person2_id", GremlinUtils.makeIid( Entity.PERSON, ldbcQuery14.person2Id() ) );
    params.put( "person_label", Entity.PERSON.getName() );

    String statement = "g.V().has(person_label, 'iid', person1_id)                                                                                                 "
      + "  .repeat(out('knows').simplePath()).until(has(person_label, 'iid', person2_id)).path()                                                    "
      + "  .union(identity(), count(local)).as('path', 'length')                                                                             "
      + "  .match(                                                                                                                           "
      + "     __.as('a').unfold().select('length').min().as('min'),                                                                          "
      + "     __.as('a').filter(eq('min', 'length')).as('shortpaths')                                                                        "
      + "  )                                                                                                                                 "
      + "  .select('path')                                                                                                                   "
      + "  .map(                                                                                                                             "
      + "    unfold().as('p1').out('knows').as('p2')                                                                                         "
      + "      .match(                                                                                                                       "
      + "        __.as('a').select('p1').in('hasCreator').out('replyOf').as('replies')                                                       "
      + "        __.as('replies').where(hasLabel('post').and().in('hasCreator').eq('p2')).as('p1p')                                          "
      + "        __.as('a').select('p2').in('hasCreator').out('replyOf').where(hasLabel('post').and().in('hasCreator').eq('p1'))             "
      + "        __.as('a').select('p1').in('hasCreator').out('replyOf').where(hasLabel('comment').and().in('hasCreator').eq('p2c'))         "
      + "        __.as('a').select('p2').in('hasCreator').out('replyOf').where(hasLabel('comment').and().in('hasCreator').eq('p1c'))         "
      + "        __.as('p1p').union(identity(), 'p2').count(local).as('postweight')                                                          "
      + "        __.as('p1c').union(identity(), 'p2c').count(local).union(identity(), constant(0.5)).mult().as('commentweight')              "
      + "        __.as('postweight').union(identity(), 'commentweight').sum().as('weight')                                                   "
      + "      ).sum().as('totalweight')                                                                                                     "
      + "  ).select('path', 'totalweight').order().by('totalweight', decr)                                                                   "

    List<Result> results = null;
    try {
      results = client.submit( statement, params ).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException( "Remote execution failed", e );
    }


    List<LdbcQuery14Result> resultList = new ArrayList<>();
    for (Result r : results) {
      HashMap map = r.get( HashMap.class );
      Path path = (Path) map.get( "path" );
      double weight = (double) map.get( "weight" );

      List<Long> idsInPath = new ArrayList<>();
      for (Object o : path) {
        Vertex v = (Vertex) o;
        idsInPath.add( GremlinUtils.getSNBId( v ) );
      }
      LdbcQuery14Result ldbcQuery14Result = new LdbcQuery14Result(
        idsInPath,
        weight );

      resultList.add( ldbcQuery14Result );
    }
    resultReporter.report( resultList.size(), resultList, ldbcQuery14 );
  }
}

