### Complex Query 12
Given person, find all messages of friends which are reply to posts that has given sublass transitively
- g.V().has('iid', 'person:3991').out('knows').as('friends').in('hasCreator').where(out('replyOf').hasLabel('post').out('hasTag').repeat(out('hasType')).until(has('name', 'Criminal')))

### Complex Query 11
Given person, get all 2
- g.V().has('iid', 'person:3991').repeat(out('knows').simplePath()).until(loops().is(gte(2))).dedup()

pattern match part
- _.as('person').outE('workAt').has('workFrom', lte('2004')).as('date').inV().as('organization').out('isLocatedIn').has('name', 'Spain')