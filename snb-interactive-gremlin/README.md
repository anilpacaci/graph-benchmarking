This repository contains [Linked Data Benchmark Council](http://www.ldbcouncil.org/)'s 
[Social Network Benchmark](http://www.ldbcouncil.org/benchmarks/snb)'s implementation for Tinkerpop3 compliant graph databases.

All SNB Interactive queries are implemented as Gremlin Traversal's and can be submitted to any TP3 Blueprint
compliant graph database behind a Gremlin Server. See [LDBC Driver](https://github.com/ldbc/ldbc_driver) for configuration and execution of the SNB Interactive Workload.

`Scripts` directory includes various Gremlin scripts to import LDBC SNB datasets into TP3 Blueprint graph databases (Neo4j, TitanDB, Sqlg).
Following can be used to import into TitanDB from Gremlin Console;

```shell
:load scripts/SNBParser.groovy
:load scripts/initTitan.groovy
graph = initializeTitan(TITAN_DB_PROPERTIES)
SNBParser.loadSNBGraph(graph, SNB_SOCIAL_NETWORK, BATCH_SIZE, REPORTING_PERIOD)
```
### Current Status 
* Complex Traversals: 12 / 14
* Short Traversals: 7/7 (Pass validation)
* Updates: 8/8 Pass Validation
