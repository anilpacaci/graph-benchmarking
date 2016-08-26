package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbException;

import java.util.Map;

public interface UpdateHandler {
    void submitQuery(String statement, Map<String, Object> params) throws DbException;
}
