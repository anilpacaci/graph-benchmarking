package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinKafkaDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinStatement;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class LdbcUpdate8Handler implements OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        KafkaProducer<String, GremlinStatement> producer = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaProducer();
        String topic = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaTopic();
        Map<String, Object> params = new HashMap<>();
        params.put("p1_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person1Id()));
        params.put("p2_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person2Id()));
        Map<String, Object> props = new HashMap<>();
        props.put( "creationDate", String.valueOf( ldbcUpdate8AddFriendship.creationDate().getTime() ) );
        params.put("props", props);
        String statement = "p1 = g.V().has('iid', p1_id).next(); " +
            "p2 = g.V().has('iid', p2_id); " +
            "p1.addEdge('knows', p2, props); " +
            "p2.addEdge('knows', p1, props);";
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);

    }
}
