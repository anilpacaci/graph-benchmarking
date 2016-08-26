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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class LdbcUpdate6Handler implements OperationHandler<LdbcUpdate6AddPost,DbConnectionState>
{

    @Override
    public void executeOperation( LdbcUpdate6AddPost ldbcUpdate6AddPost,
            DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        KafkaProducer<String, GremlinStatement> producer = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaProducer();
        String topic = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaTopic();

        Map<String,Object> params = new HashMap<>();
        params.put("vlabel", Entity.POST.getName());
        params.put("post_id", GremlinUtils.makeIid( Entity.POST, ldbcUpdate6AddPost.postId() ) );
        params.put("image_file", ldbcUpdate6AddPost.imageFile() );
        params.put("creation_date", String.valueOf( ldbcUpdate6AddPost.creationDate().getTime() ) );
        params.put("location_ip", ldbcUpdate6AddPost.locationIp() );
        params.put("browser_used", ldbcUpdate6AddPost.browserUsed() );
        params.put("language", ldbcUpdate6AddPost.language() );
        params.put("content", ldbcUpdate6AddPost.content() );
        params.put("length", String.valueOf( ldbcUpdate6AddPost.length() ) );

        params.put("creator_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate6AddPost.authorPersonId() ) );
        params.put("forum_id", GremlinUtils.makeIid( Entity.FORUM, ldbcUpdate6AddPost.forumId() ) );
        params.put("country_id", GremlinUtils.makeIid( Entity.PLACE, ldbcUpdate6AddPost.countryId() ) );

        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate6AddPost.tagIds()));

        params.put("person_label", Entity.PERSON.getName());
        params.put("forum_label", Entity.FORUM.getName());
        params.put("place_label", Entity.PLACE.getName());
        params.put("tag_label", Entity.TAG.getName());

        String statement = "post = g.addV(label, vlabel).property('iid', post_id)" +
                ".property('imageFile', image_file)" +
                ".property('creationDate', creation_date)" +
                ".property('locationIP', location_ip)" +
                ".property('browserUsed', browser_used)" +
                ".property('language', language)" +
                ".property('content', content)" +
                ".property('length', length).next(); " +
                "creator = g.V().has(person_label, 'iid', creator_id).next(); " +
                "forum = g.V().has(forum_label, 'iid', forum_id).next(); " +
                "country = g.V().has(place_label, 'iid', country_id).next(); " +
                "post.addEdge('hasCreator', creator); " +
                "forum.addEdge('containerOf', post); " +
                "post.addEdge('isLocatedIn', country);" +
                "tag_ids.forEach{t -> tag = g.V().has(tag_label, 'iid', t).next(); post.addEdge('hasTag', tag); };";
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate6AddPost );

    }
}
