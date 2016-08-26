package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinKafkaDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 */
public class LdbcShortQuery4Handler implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinKafkaDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("label1", Entity.POST.getName());
        params.put("label2", Entity.COMMENT.getName());
        params.put("post_id", GremlinUtils.makeIid(Entity.POST, ldbcShortQuery4MessageContent.messageId()));
        params.put("comment_id", GremlinUtils.makeIid(Entity.COMMENT, ldbcShortQuery4MessageContent.messageId()));

        String statement = "g.V().hasLabel(label1, label2).has('iid', within(post_id, comment_id))";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Vertex message = results.get(0).getVertex();

        String content = message.<String>property( "content" ).value();
        if(content == null || content.isEmpty()) {
            content = message.<String>property("imageFile").value();
        }

        LdbcShortQuery4MessageContentResult result =
            new LdbcShortQuery4MessageContentResult(
                content,
                Long.decode( message.<String>property( "creationDate" ).value() ) );

        resultReporter.report(1, result, ldbcShortQuery4MessageContent);
    }
}
