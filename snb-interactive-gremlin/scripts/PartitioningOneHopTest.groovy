import com.opencsv.CSVWriter
import com.thinkaurelius.titan.core.TitanFactory
import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Graph


import java.util.concurrent.TimeUnit

/**
 * Created by apacaci on 2/5/17.
 *
 * Helper Groovy script to run 1-hop friendship query over TitanDb
 * It measures the time to retrieve 1-hop friendship and reports the partitions of neighbours
 */
class PartitioningOneHopTest {

    private static String[] CSV_HEADERS = ["IID", "VERTEX_PARTITION", "NEIGHBOUR_COUNT", "TOTAL_PARTITION", "PARTITION_0", "PARTITION_1", "PARTITION_2", "PARTITION_3", "NEIGHBOURHOOD_RETRIEVAL_DURATION", "TOTAL_DURATION" ]

    static void run(Graph graph, String parametersFile, String outputFile) {

        GraphTraversalSource g = graph.traversal()

        LineIterator it = FileUtils.lineIterator(new File(parametersFile))

        // skip the first line
        it.next()

        // create the CSV Writer
        FileWriter fileWriter = new FileWriter(outputFile)
        CSVWriter csvPrinter = new CSVWriter(fileWriter)
        csvPrinter.writeNext(CSV_HEADERS)


        while(it.hasNext()) {
            // we know that query_1_param.txt has iid as first parameter
            String iid = it.nextLine().split('\\|')[0]

            StandardTraversalMetrics metrics = g.V().has('iid', 'person:' + iid).out('knows').properties.profile().cap(TraversalMetrics.METRICS_KEY).next()
            Long vertexId = (Long) g.V().has('iid', 'person:' + iid).next().id()
            Long partitionId = getPartitionId(vertexId)

            long totalQueryDurationInMicroSeconds = metrics.getDuration(TimeUnit.MICROSECONDS)
            // index 2 corresponds to valueMap step, where properties of each neighbour is actually retrieved from backend
            long neighbourhoodRetrievalInMicroSeconds = metrics.getMetrics(2).getDuration(TimeUnit.MICROSECONDS)

            // index 1 corresponds to out step
            int neighbourhoodCount = metrics.getMetrics(1).getCount('elementCount')

            Map neighbourPartitionCounts = g.V(vertexId).out('knows').id().map{id -> getPartitionId(id.get())}.groupCount().next();
            int totalPartitions = neighbourPartitionCounts.size()

            List<String> queryRecord = new ArrayList();
            queryRecord.add(iid)
            queryRecord.add(partitionId.toString())
            queryRecord.add(neighbourhoodCount.toString())
            queryRecord.add(totalPartitions.toString())
            for(Long i = 0 ; i < 4 ; i++) {
                queryRecord.add(neighbourPartitionCounts.get(i).toString())
            }
            queryRecord.add(neighbourhoodRetrievalInMicroSeconds.toString())
            queryRecord.add(totalQueryDurationInMicroSeconds.toString())

            // add record to CSV
            csvPrinter.writeNext(queryRecord.toArray(new String[0]))

            // since it is read-only rollback is a less expensive option
            g.tx().rollback()

            if(totalPartitions == 1)
            println(String.format("Vertex: %s \t Partition: %d \t Count: %d \t TotalDuration: %d \t Neighbourhood Duration: %d", iid, partitionId, neighbourhoodCount, totalQueryDurationInMicroSeconds, neighbourhoodRetrievalInMicroSeconds))
        }

        // close csvWriter
        csvPrinter.close()
    }

    static void run(String graphConfigurationFile, String parametersFile, String outputFile) {
        Configuration graphConfig = new PropertiesConfiguration(graphConfigurationFile)
        Graph graph = TitanFactory.open(graphConfig)
        run(graph, parametersFile, outputFile)
    }

    static Long getPartitionId(Long id) {
        // because normal user vertex padding is 3 and partitionId bound is 3
        return ( id >>> 3 ) & 3
    }

}
