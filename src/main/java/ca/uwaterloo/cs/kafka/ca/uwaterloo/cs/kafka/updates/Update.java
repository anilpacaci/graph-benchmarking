package ca.uwaterloo.cs.kafka.ca.uwaterloo.cs.kafka.updates;

import java.io.Serializable;

public class Update implements Serializable
{
    String updateQuery;
    String csvLine;
}
