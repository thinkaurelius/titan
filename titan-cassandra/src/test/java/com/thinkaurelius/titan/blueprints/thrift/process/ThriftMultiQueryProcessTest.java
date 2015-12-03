package com.thinkaurelius.titan.blueprints.thrift.process;

import com.thinkaurelius.titan.blueprints.thrift.ThriftMultiQueryGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ThriftMultiQueryGraphProvider.class, graph = TitanGraph.class)
public class ThriftMultiQueryProcessTest {
}
