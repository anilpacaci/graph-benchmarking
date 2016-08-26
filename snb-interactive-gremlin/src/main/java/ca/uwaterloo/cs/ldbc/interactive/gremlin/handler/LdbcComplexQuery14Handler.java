package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;

public class LdbcComplexQuery14Handler implements OperationHandler<LdbcQuery14, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery14 ldbcQuery14, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        resultReporter.report(0, null, ldbcQuery14);
    }
}
