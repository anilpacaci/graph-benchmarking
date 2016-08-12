package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;

public class LdbcComplexQuery1Handler implements OperationHandler<LdbcQuery1, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery1 ldbcQuery1, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        resultReporter.report(0, null, ldbcQuery1);
    }
}
