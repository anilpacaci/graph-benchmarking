### Complex Query 12
Given person, find all messages of friends which are reply to posts that has given sublass transitively
- g.V().has('iid', 'person:3991').out('knows').as('friends').in('hasCreator').where(out('replyOf').hasLabel('post').out('hasTag').repeat(out('hasType')).until(has('name', 'Criminal')))

### Complex Query 11
Given person, get all 2
- g.V().has('iid', 'person:3991').repeat(out('knows').simplePath()).until(loops().is(gte(2))).dedup()

pattern match part
- __.as('person').outE('workAt').has('workFrom', lte('2004')).as('date').inV().as('organization').out('isLocatedIn').has('name', 'Spain')

### Complex Query 10

+"Calendar cal = Calendar.getInstance();                                                              "
+"Calendar lowercal = Calendar.getInstance();                                                         "
+"Calendar highercal = Calendar.getInstance();                                                        "
+"g.V().has('iid', 'person:{}').as('startPerson')                                                     "
+"   .out('hasInterest').aggregate('persontags')                                                      "
+"   .select('startPerson')                                                                           "
+"   .repeat(out()).times(2).path().by('knows')                                                       "
+"   .filter{ it => {                                                                                 "
+"       ts = it.get().value('birthDay')                                                              "
+"       cal.setTime(new java.util.Date((long)ts**1000));                                             "
+"       int day = cal.get(Calendar.DAY_OF_MONTH);                                                    "
+"       int month = cal.get(Calendar.MONTH);                                                         "
+"       int year = cal.get(Calendar.YEAR);                                                           "
+"                                                                                                    "
+"       if (day < 21) {                                                                              "
+"         lowercal.set(year, month-1, 21)                                                            "
+"         highercal.set(year, month, 22)                                                             "
+"       } else {                                                                                     "
+"         lowercal.set(year, month, 21)                                                              "
+"         highercal.set(year, month+1, 22)                                                           "
+"       }                                                                                            "
+"                                                                                                    "
+"       return lowercal.compareTo(cal) <= 0 && highercal.compareTo(cal) > 0;                         "
+"     }                                                                                              "
+"   }                                                                                                "
+"   .has('birthDay',inside((year, month, 21),(year, month+1, 22))).as('fof')                         "
+"   .map(                                                                                            "
+"     in('hasCreator').hasLabel('post').where(out('hasTag')).match(                                  "
+"        __.as('tag').where(within('persontags')).count().as('commonCount')                          "
+"        __.as('tag').where(without('persontags')).count().as('uncommonCount')                       "
+"        __.as('uncommonCount').map(union(identity(), constant(-1))).fold(1, mult)                   "
+"        __.as('commonCount').map(union(identity(), select('uncommonCount')).sum()).as('similarity') "
+"        )                                                                                           "
+"     )                                                                                              "
+"   ).select('fof', 'similarity').order().by('similarity').limit(10)                                 "

### Complex Query 14

g = (graph = Neo4jGraph.open('/home/apacaci/Projects/jimmy/neo4j-community-2.2.2/data/gremlin/validation') ).traversal()

"static double calculateWeight(GraphTraversalSource g, Long v1, Long v2) {long postForward = g.V(v1).in('hasCreator').hasLabel('post').in('replyOf').out('hasCreator').hasId(v2).count().next(); long postBackward = g.V(v2).in('hasCreator').hasLabel('post').in('replyOf').out('hasCreator').hasId(v1).count().next(); long commentForward = g.V(v1).in('hasCreator').hasLabel('comment').in('replyOf').out('hasCreator').hasId(v2).count().next(); long  commentBackward = g.V(v2).in('hasCreator').hasLabel('comment').in('replyOf').out('hasCreator').hasId(v1).count().next(); long score = postForward + postBackward + 0.5 * (commentForward + commentBackward); return score;} "
+"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "
+"map = [:]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             "
+"shortestPathLength = g.V().has('person', 'iid', 'person:2202').repeat(out('knows').simplePath()).until(has('person', 'iid', 'person:1979')).path().limit(1).count(local).next();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "
+"g.V().has('person', 'iid', 'person:2202').repeat(out('knows').simplePath()).until(loops().is(gte(shortestPathLength - 1))).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           "
+"    filter(has('person', 'iid', 'person:1979')).path().as('shortestPaths').map{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       "
+"        path = it.get();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              "
+"        totalScore = 0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                "
+"        for(int i = 0; i < path.size() - 2; i++) {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    "
+"            totalScore += calculateWeight(g, path.get(i).id(), path.get(i + 1).id())                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  "
+"        }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             "
+"        map.put(path, totalScore)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     "
+"    }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 "
