package com.thinkaurelius.titan.blueprints.process;


import com.thinkaurelius.titan.blueprints.BerkeleyMultiQueryGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = BerkeleyMultiQueryGraphProvider.class, graph = TitanGraph.class)
public class BerkeleyMultiQueryTitanProcessTest {
}
