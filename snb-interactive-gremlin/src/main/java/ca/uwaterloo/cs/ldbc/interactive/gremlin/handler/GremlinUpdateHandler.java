package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbException;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GremlinUpdateHandler implements UpdateHandler
{
    private Client client;
    public GremlinUpdateHandler( Client client ) {
        this.client = client;
    }
    @Override
    public void submitQuery(String statement, Map<String, Object> params) throws DbException
    {
        try {
            client.submit( statement, params ).all().get();
        } catch (InterruptedException | ExecutionException e)
        {
            throw new DbException("remote execution failed", e);
        }
    }
}
