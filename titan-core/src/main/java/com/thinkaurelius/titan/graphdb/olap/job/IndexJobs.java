package com.thinkaurelius.titan.graphdb.olap.job;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.StandardScanner;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import org.apache.commons.configuration.BaseConfiguration;

// TODO refactor this as part of #756
public class IndexJobs {

    public ScanMetrics run(final StandardTitanGraph graph, ScanJob sj, Configuration jobConf) {
        StandardScanner.Builder scanBuilder = graph.getBackend().buildEdgeScanJob();
        scanBuilder.setJobId(sj.toString());
        scanBuilder.setNumProcessingThreads(1);
        scanBuilder.setWorkBlockSize(4096);
        scanBuilder.setJob(sj);
        scanBuilder.setJobConfiguration(jobConf);

        try {
            ScanMetrics jobResult = scanBuilder.execute().get();
            long failures = jobResult.get(ScanMetrics.Metric.FAILURE);
            if (failures>0) {
                throw new TitanException("Failed to process ["+failures+"]");
            }
            return jobResult;
        } catch (Exception e) {
            throw new TitanException(e);
        }
    }

    public ScanMetrics run(final StandardTitanGraph g, VertexScanJob vsj, Configuration jobConf) {
        return run(g, VertexJobConverter.convert(g, vsj), jobConf);
    }

    public ScanMetrics repair(final StandardTitanGraph g, String indexName, String indexRelationType) {

        ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.JOB_NS,
                new CommonsConfiguration(new BaseConfiguration()), BasicConfiguration.Restriction.NONE);
        mc.set(com.thinkaurelius.titan.graphdb.olap.job.IndexUpdateJob.INDEX_NAME, indexName);
        mc.set(com.thinkaurelius.titan.graphdb.olap.job.IndexUpdateJob.INDEX_RELATION_TYPE, indexRelationType);
        mc.set(GraphDatabaseConfiguration.JOB_START_TIME, System.currentTimeMillis());

        return run(g, new IndexRepairJob(), mc);
    }

    /*

    // Gremlin: load-defineindex-reindex-query
    // TODO adapt to a test and/or asciidoc code snippet

g = TitanFactory.open('conf/titan-berkeleyje.properties')
stream = new FileInputStream('data/tinkerpop-modern.gio')
reader = KryoReader.build().create()
reader.readGraph(stream, g)
g.tx().commit()

g.V().has('name', 'lop')
g.tx().rollback()

m = g.openManagement()
m.buildIndex('names', Vertex.class).addKey(m.getPropertyKey('name')).buildCompositeIndex()
m.commit()
g.tx().commit()

com.thinkaurelius.titan.graphdb.database.management.ManagementSystem.awaitGraphIndexStatus(g, 'names').status(SchemaStatus.REGISTERED).call()

new com.thinkaurelius.titan.graphdb.olap.job.IndexJobs().repair(g, "names", "")
g.tx().commit()
m = g.openManagement()
i = m.getGraphIndex('names')
m.updateIndex(i, SchemaAction.ENABLE_INDEX)
m.commit()

com.thinkaurelius.titan.graphdb.database.management.ManagementSystem.awaitGraphIndexStatus(g, 'names').status(SchemaStatus.ENABLED).call()

g.V().has('name', 'lop')
g.tx().rollback()



     */
}
