### Graph Database Benchmarking Effort 
This repository contains an Tinkerpop3 compliant implementation of LDBC SNB Interactive Workload and pointers to various resources for benchamrking graph databases for this new workload.

##### SNB Interactive Gremlin  
Tinkerpop3 Blueprint compliant implementation of [Linked Data Benchmark Council](http://www.ldbcouncil.org/)'s 
[Social Network Benchmark Benchmark](http://www.ldbcouncil.org/benchmarks/snb)

##### Kafka Integration
We are in effort to integrate Kafka into LDBC Driver to emulate real-time updates on SUT. [LDBC Driver](https://github.com/anilpacaci/ldbc_driver) is WIP of this integration.

##### Various LDBC SNB Implementations

* [Reference Implementations](https://github.com/anilpacaci/ldbc_snb_implementations) Forked from LDBC repo[https://github.com/ldbc/ldbc_snb_implementations].
  * Bugs in loader scripts have been fixed
  * New scripts to correctly define SPARQL prefixes
  * SPARQL Complex Traversals needs fixing (Complex query 2)
  * TitanDB supports only TP2. Gremlin implementation should be used
  
* [PlatformLab Implementations](https://github.com/anilpacaci/ldbc-snb-impls) TP3 compliant TitanDB and Neo4j implementation from [PlatformLab](https://github.com/platformLab/)
  * No Complex Traversals, No validation on TitanDB implementation
  * Neo4j Cypher queries (passing validation)
  
##### Tinkerpop3 Implementations
* [Sqlg](https://github.com/pietermartin/sqlg) Implementation of TP3 Structure API on Postgres 
  * [SqlgSNBImporter](snb-interactive-gremlin/scripts/SqlgSNBImporter.groovy) can succesfully import LDBC SNB Dataset into Sqlg Graph over Postgres
  * Optimizes Traversal steps
* [Sqlgraph](https://github.com/aliceranzhou/sqlgraph) Simple - inefficient implementation of TP3 Structure API over RDBMS by Alice and Anil
  * Does not support any bulk loading features
  * No optimization what so ever
