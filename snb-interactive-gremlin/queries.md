### Complex Query 12
Given person, find all messages of friends which are reply to posts that has given sublass transitively
- g.V().has('iid', 'person:3991').out('knows').as('friends').in('hasCreator').where(out('replyOf').hasLabel('post').out('hasTag').repeat(out('hasType')).until(has('name', 'Criminal')))

