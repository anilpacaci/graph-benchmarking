package ca.uwaterloo.cs.kafka.utils;

/**
 * Created by apacaci on 5/16/16.
 */
public class GraphEdge {

    private String sourceVertex;
    private String targetVertex;

    public GraphEdge() {
    }

    public GraphEdge(String sourceVertex, String targetVertex) {
        this.sourceVertex = sourceVertex;
        this.targetVertex = targetVertex;
    }

    public String getSourceVertex() {
        return sourceVertex;
    }

    public void setSourceVertex(String sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public String getTargetVertex() {
        return targetVertex;
    }

    public void setTargetVertex(String targetVertex) {
        this.targetVertex = targetVertex;
    }
}
