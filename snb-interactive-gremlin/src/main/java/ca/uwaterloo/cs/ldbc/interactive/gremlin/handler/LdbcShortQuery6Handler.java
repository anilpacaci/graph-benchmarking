package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;

/**
 * Created by apacaci on 7/20/16.
 */
public class LdbcShortQuery6Handler implements OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

    }
}
