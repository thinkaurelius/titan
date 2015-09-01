package com.thinkaurelius.titan.blueprints.process.groovy;

import com.thinkaurelius.titan.blueprints.BerkeleyGraphComputerProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * @author Bryn Cooke
 */
@RunWith(GroovyProcessComputerSuite.class)
@GraphProviderClass(provider = BerkeleyGraphComputerProvider.class, graph = TitanGraph.class)
public class BerkeleyTitanGroovyProcessComputerTest {
}