package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import java.io.Serializable;
import java.util.Map;

public class GremlinStatement implements Serializable
{
    String statement;
    Map<String, Object> params;

    public GremlinStatement(){
    }

    public GremlinStatement(String statement, Map<String, Object> params){
        this.statement = statement;
        this.params = params;
    }
    public String getStatement() {
        return statement;
    }
    public Map<String, Object> getParams() {
        return params;
    }
    public void setStatement(String statement) {
        this.statement = statement;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
