### Complex Query 12
Given person, find all messages of friends which are reply to posts that has given sublass transitively
- g.V().has('iid', 'person:3991').out('knows').as('friends').in('hasCreator').where(out('replyOf').hasLabel('post').out('hasTag').repeat(out('hasType')).until(has('name', 'Criminal')))

### Complex Query 11
Given person, get all 2
- g.V().has('iid', 'person:3991').repeat(out('knows').simplePath()).until(loops().is(gte(2))).dedup()

pattern match part
- __.as('person').outE('workAt').has('workFrom', lte('2004')).as('date').inV().as('organization').out('isLocatedIn').has('name', 'Spain')

### Complex Query 10

g.V().has('iid', 'person:{}').as('startPerson')
   .out('hasInterest').aggregate('persontags')
   .select('startPerson')
   .repeat(out()).times(2).path().by('knows')
   .has('birthDay',inside((year, month, 21),(year, month+1, 22))).as('fof')
   .map(
     in('hasCreator').hasLabel('post').where(out('hasTag')).match(
        __.as('tag').where(within('persontags')).count().as('commonCount')
        __.as('tag').where(without('persontags')).count().as('uncommonCount')
        __.as('uncommonCount').map(union(identity(), constant(-1))).fold(1, mult)
        __.as('commonCount').map(union(identity(), select('uncommonCount')).sum()).as('similarity')
        )
     )
   ).select('fof', 'similarity').order().by('similarity').limit(10)

### Complex Query 14

g.V().has('id', 'person:{person1}')
  .repeat(out('knows').simplePath()).until(has('id', 'person:{person2}')).path()
  .union(identity(), count(local)).as('path', 'length')
  .match(
     __.as('a').unfold().select('length').min().as('min'),
     __.as('a').filter(eq('min', 'length')).as('shortpaths')
  )
  .select('path')
  .map(
    unfold().as('p1').out('knows').as('p2')
      .match(
        __.as('a').select('p1').in('hasCreator').out('replyOf').as('replies')
        __.as('replies').where(hasLabel('post').and().in('hasCreator').eq('p2')).as('p1p')
        __.as('a').select('p2').in('hasCreator').out('replyOf').where(hasLabel('post').and().in('hasCreator').eq('p1'))
        __.as('a').select('p1').in('hasCreator').out('replyOf').where(hasLabel('comment').and().in('hasCreator').eq('p2c'))
        __.as('a').select('p2').in('hasCreator').out('replyOf').where(hasLabel('comment').and().in('hasCreator').eq('p1c'))
        __.as('p1p').union(identity(), 'p2').count(local).as('postweight')
        __.as('p1c').union(identity(), 'p2c').count(local).union(identity(), constant(0.5)).mult().as('commentweight')
        __.as('postweight').union(identity(), 'commentweight').sum().as('weight')
      ).sum().as('totalweight')
  ).select('path', 'totalweight').order().by('totalweight', decr)
