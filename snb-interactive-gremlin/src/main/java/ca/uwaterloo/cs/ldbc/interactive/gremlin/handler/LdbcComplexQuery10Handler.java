package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;

public class LdbcComplexQuery10Handler implements OperationHandler<LdbcQuery10, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery10 ldbcQuery10, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        resultReporter.report(0, null, ldbcQuery10);
    }
}
