
import com.thinkaurelius.titan.core.TitanFactory
import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Graph

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.Timer
import java.util.TimerTask

/**
 * Created by apacaci on 2/5/17.
 *
 * Helper Groovy script to run 1-hop friendship query over TitanDb
 * It measures the time to retrieve 1-hop friendship and reports the partitions of neighbours
 */
class OneHopThroughput {

    private String[] CSV_HEADERS = ["IID", "VERTEX_PARTITION", "NEIGHBOUR_COUNT", "TOTAL_PARTITION", "PARTITION_0", "PARTITION_1", "PARTITION_2", "PARTITION_3", "NEIGHBOURHOOD_RETRIEVAL_DURATION", "TOTAL_DURATION" ]

    Graph graph
    String parameters
    String outputFile
    int numberOfClients

    OneHopThroughput(Graph graph, String parameters, String outputFile, int numberOfClients) {
        this.graph = graph
        this.parameters = parameters
        this.outputFile = outputFile
        this.numberOfClients = numberOfClients
    }

    void execute() {

        GraphTraversalSource g = graph.traversal()

        def counter = new AtomicInteger(0)
        def iidQueue = new ConcurrentLinkedQueue<String>()
        def parametersFile = new File(parameters)

        def totalQueries = populateQueueFromFile(parametersFile, iidQueue)

        def executorService = Executors.newFixedThreadPool(numberOfClients)

        int tempCount = 0
        int secondCounter = 0

        def timer = new Timer()
        timer.schedule(new TimerTask() {
            @Override
            void run() {
                int currentCount = counter.get()
                println(String.format("Second: %d\t- Throughput: %d", secondCounter++, currentCount - tempCount))
                tempCount = currentCount
            }
        }, 0, 1000)


        def tasks = new ArrayList<QueryExecutor>()

        for(int i = 0 ; i < numberOfClients ; i++) {
            tasks.add(new QueryExecutor(graph, iidQueue, counter))
        }

        long start = System.currentTimeMillis()
        executorService.invokeAll(tasks)
        long timeElapsed = System.currentTimeMillis() - start
        timer.cancel()

        println(String.format("Total time in ms: %d , # of queries: %d , tput: %f", timeElapsed, totalQueries, totalQueries / (timeElapsed / 1000)))

        graph.close()
    }


    static int populateQueueFromFile(File parametersFile, Queue<String> queue) {
        LineIterator it = FileUtils.lineIterator(parametersFile)

        // skip the first line
        it.next()

        while(it.hasNext()) {
            // we know that query_1_param.txt has iid as first parameter
            String iid = it.nextLine().split('\\|')[0]
            queue.add(iid)
        }
        return queue.size()
    }

    static void run(String graphConfigurationFile, String parametersFile, String outputFile, int numberOfClient) {
        Configuration graphConfig = new PropertiesConfiguration(graphConfigurationFile)
        Graph graph = TitanFactory.open(graphConfig)
        def test = new OneHopThroughput(graph, parametersFile, outputFile, numberOfClient)
        test.execute()

    }

    static void main(String[] args) {
        run(args[0], args[1], args[2], Integer.parseInt(args[3]))
    }

    class QueryExecutor implements Callable {

        Graph graph
        ConcurrentLinkedQueue<String> iidQueue
        AtomicInteger queryCounter

        QueryExecutor(Graph graph, ConcurrentLinkedQueue<String> iidQueue, AtomicInteger queryCounter) {
            this.graph = graph
            this.iidQueue = iidQueue
            this.queryCounter = queryCounter
        }

        @Override
        Object call() {
            def g = graph.traversal()
            while (true) {
                def id = iidQueue.poll()
                if(id == null) {
                    return
                }

                // we obtained valid id, run query
                def traversal = g.V().has('iid', 'person:' + id).out('knows').properties().profile().cap(TraversalMetrics.METRICS_KEY).next()

                queryCounter.incrementAndGet()
                g.tx().rollback()
                // may be we can parse the results and generate more iids
            }
        }

        void populateQueueFromResult(DefaultGraphTraversal traversal) {
            while(traversal.hasNext()) {
                def p = traversal.next()
                if(p.key().equals("iid")) {
                    def resultIID = (String) p.value()
                    def newID = resultIID.split(":")[1]
                    iidQueue.add(newID)
                }
            }
        }

    }

}
