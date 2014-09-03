package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.EdgeCopyMapReduce;
import com.thinkaurelius.titan.hadoop.formats.MapReduceFormat;
import com.thinkaurelius.titan.hadoop.mapreduce.IdentityMap;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.BackFilterMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.CyclicPathFilterMap;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.DuplicateFilterMap;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.FilterMap;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.IntervalFilterMap;
import com.thinkaurelius.titan.hadoop.mapreduce.filter.PropertyFilterMap;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.CommitEdgesMap;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.CommitVerticesMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.GroupCountMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.LinkMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.ScriptMap;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.SideEffectMap;
import com.thinkaurelius.titan.hadoop.mapreduce.sideeffect.ValueGroupCountMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.EdgesMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.EdgesVerticesMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.OrderMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.PathMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.PropertyMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.PropertyMapMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.TransformMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.VertexMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.VerticesEdgesMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.VerticesMap;
import com.thinkaurelius.titan.hadoop.mapreduce.transform.VerticesVerticesMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.util.CountMapReduce;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.transform.TransformPipe;
import com.tinkerpop.pipes.util.structures.Pair;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.*;


/**
 * A HadoopPipeline defines a breadth-first traversal through a property graph representation.
 * Gremlin/Hadoop compiles down to a HadoopPipeline which is ultimately a series of MapReduce jobs.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopPipeline {

    private static final Logger log =
            LoggerFactory.getLogger(HadoopPipeline.class);

    // used to validate closure parse tree
    protected static final ScriptEngine engine = new GroovyScriptEngineImpl();
    public static final String PIPELINE_IS_LOCKED = "No more steps are possible as pipeline is locked";

    protected final HadoopCompiler compiler;
    protected final HadoopGraph graph;
    protected final State state;

    protected final List<String> stringRepresentation = new ArrayList<String>();

    private Compare convert(final com.tinkerpop.gremlin.Tokens.T compare) {
        if (compare.equals(com.tinkerpop.gremlin.Tokens.T.eq))
            return Compare.EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.neq))
            return Compare.NOT_EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.gt))
            return Compare.GREATER_THAN;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.gte))
            return Compare.GREATER_THAN_EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.lt))
            return Compare.LESS_THAN;
        else
            return Compare.LESS_THAN_EQUAL;
    }

    protected class State {
        private Class<? extends Element> elementType;
        private String propertyKey;
        private Class<? extends WritableComparable> propertyType;
        private int step = -1;
        private boolean locked = false;
        private Map<String, Integer> namedSteps = new HashMap<String, Integer>();

        public State set(Class<? extends Element> elementType) {
            if (!elementType.equals(Vertex.class) && !elementType.equals(Edge.class))
                throw new IllegalArgumentException("The element class type must be either Vertex or Edge");

            this.elementType = elementType;
            return this;
        }

        public Class<? extends Element> getElementType() {
            return this.elementType;
        }

        public boolean atVertex() {
            if (null == this.elementType)
                throw new IllegalStateException("No element type can be inferred: start vertices (or edges) set must be defined");
            return this.elementType.equals(Vertex.class);
        }

        public State setProperty(final String key, final Class type) {
            this.propertyKey = key;
            this.propertyType = convertJavaToHadoop(type);
            return this;
        }

        public Pair<String, Class<? extends WritableComparable>> popProperty() {
            if (null == this.propertyKey)
                return null;
            Pair<String, Class<? extends WritableComparable>> pair = new Pair<String, Class<? extends WritableComparable>>(this.propertyKey, this.propertyType);
            this.propertyKey = null;
            this.propertyType = null;
            return pair;
        }

        public int incrStep() {
            return ++this.step;
        }

        public int getStep() {
            return this.step;
        }

        public void assertNotLocked() {
            if (this.locked) throw new IllegalStateException(PIPELINE_IS_LOCKED);
        }

        public void assertNoProperty() {
            if (this.propertyKey != null)
                throw new IllegalStateException("This step can not follow a property reference");
        }

        public void assertAtVertex() {
            if (!this.atVertex())
                throw new IllegalStateException("This step can not follow an edge-based step");
        }

        public void assertAtEdge() {
            if (this.atVertex())
                throw new IllegalStateException("This step can not follow a vertex-based step");
        }

        public boolean isLocked() {
            return this.locked;
        }

        public void lock() {
            this.locked = true;
        }

        public void addStep(final String name) {
            if (this.step == -1)
                throw new IllegalArgumentException("There is no previous step to name");

            this.namedSteps.put(name, this.step);
        }

        public int getStep(final String name) {
            final Integer i = this.namedSteps.get(name);
            if (null == i)
                throw new IllegalArgumentException("There is no step identified by: " + name);
            else
                return i;
        }
    }


    ////////////////////////////////
    ////////////////////////////////
    ////////////////////////////////

    /**
     * Construct a HadoopPipeline
     *
     * @param graph the HadoopGraph that is the source of the traversal
     */
    public HadoopPipeline(final HadoopGraph graph) {
        this.graph = graph;
        this.compiler = HadoopCompatLoader.getCompat().newCompiler(graph);
        this.state = new State();

        if (MapReduceFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
            try {
                ((Class<? extends MapReduceFormat>) this.graph.getGraphInputFormat()).getConstructor().newInstance().addMapReduceJobs(this.compiler);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }



        if (graph.hasEdgeCopyDirection()) {
            Direction ecDir = graph.getEdgeCopyDirection();
            this.compiler.addMapReduce(EdgeCopyMapReduce.Map.class,
                    null,
                    EdgeCopyMapReduce.Reduce.class,
                    null,
                    LongWritable.class,
                    Holder.class,
                    NullWritable.class,
                    FaunusVertex.class,
                    EdgeCopyMapReduce.createConfiguration(ecDir));
        }
    }

    //////// TRANSFORMS

    /**
     * The identity step does not alter the graph in anyway.
     * It has the benefit of emitting various useful graph statistic counters.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline _() {
        this.state.assertNotLocked();
        this.compiler.addMap(IdentityMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                IdentityMap.createConfiguration());
        makeMapReduceString(IdentityMap.class);
        return this;
    }

    /**
     * Apply the provided closure to the current element and emit the result.
     *
     * @param closure the closure to apply to the element
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline transform(final String closure) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(TransformMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                TransformMap.createConfiguration(this.state.getElementType(), this.validateClosure(closure)));

        this.state.lock();
        makeMapReduceString(TransformMap.class);
        return this;
    }

    /**
     * Start a traversal at all vertices in the graph.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline V() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.set(Vertex.class);

        this.compiler.addMap(VerticesMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                VerticesMap.createConfiguration(this.state.incrStep() != 0));

        makeMapReduceString(VerticesMap.class);
        return this;
    }

    /**
     * Start a traversal at all edges in the graph.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline E() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.set(Edge.class);

        this.compiler.addMap(EdgesMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                EdgesMap.createConfiguration(this.state.incrStep() != 0));

        makeMapReduceString(EdgesMap.class);
        return this;
    }

    /**
     * Start a traversal at the vertices identified by the provided ids.
     *
     * @param ids the long ids of the vertices to start the traversal from
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline v(final long... ids) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.state.set(Vertex.class);
        this.state.incrStep();

        this.compiler.addMap(VertexMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                VertexMap.createConfiguration(ids));

        makeMapReduceString(VertexMap.class);
        return this;
    }

    /**
     * Take outgoing labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline out(final String... labels) {
        return this.inOutBoth(OUT, labels);
    }

    /**
     * Take incoming labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline in(final String... labels) {
        return this.inOutBoth(IN, labels);
    }

    /**
     * Take both incoming and outgoing labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline both(final String... labels) {
        return this.inOutBoth(BOTH, labels);
    }

    private HadoopPipeline inOutBoth(final Direction direction, final String... labels) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.assertAtVertex();
        this.state.incrStep();

        this.compiler.addMapReduce(VerticesVerticesMapReduce.Map.class,
                null,
                VerticesVerticesMapReduce.Reduce.class,
                null,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                VerticesVerticesMapReduce.createConfiguration(direction, labels));
        this.state.set(Vertex.class);
        makeMapReduceString(VerticesVerticesMapReduce.class, direction.name(), Arrays.asList(labels));
        return this;

    }

    /**
     * Take outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline outE(final String... labels) {
        return this.inOutBothE(OUT, labels);
    }

    /**
     * Take incoming labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline inE(final String... labels) {
        return this.inOutBothE(IN, labels);
    }

    /**
     * Take both incoming and outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline bothE(final String... labels) {
        return this.inOutBothE(BOTH, labels);
    }

    private HadoopPipeline inOutBothE(final Direction direction, final String... labels) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.assertAtVertex();
        this.state.incrStep();

        this.compiler.addMapReduce(VerticesEdgesMapReduce.Map.class,
                null,
                VerticesEdgesMapReduce.Reduce.class,
                null,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                VerticesEdgesMapReduce.createConfiguration(direction, labels));
        this.state.set(Edge.class);
        makeMapReduceString(VerticesEdgesMapReduce.class, direction.name(), Arrays.asList(labels));
        return this;
    }

    /**
     * Go to the outgoing/tail vertex of the edge.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline outV() {
        return this.inOutBothV(OUT);
    }

    /**
     * Go to the incoming/head vertex of the edge.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline inV() {
        return this.inOutBothV(IN);
    }

    /**
     * Go to both the incoming/head and outgoing/tail vertices of the edge.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline bothV() {
        return this.inOutBothV(BOTH);
    }

    private HadoopPipeline inOutBothV(final Direction direction) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.assertAtEdge();
        this.state.incrStep();

        this.compiler.addMap(EdgesVerticesMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                EdgesVerticesMap.createConfiguration(direction));
        this.state.set(Vertex.class);
        makeMapReduceString(EdgesVerticesMap.class, direction.name());
        return this;
    }

    /**
     * Emit the property value of an element.
     *
     * @param key  the key identifying the property
     * @param type the class of the property value (so Hadoop can intelligently handle the result)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline property(final String key, final Class type) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.setProperty(key, type);
        return this;
    }

    /**
     * Emit the property value of an element.
     *
     * @param key the key identifying the property
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline property(final String key) {
        return this.property(key, String.class);
    }

    /**
     * Emit a string representation of the property map.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline map() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(PropertyMapMap.Map.class,
                LongWritable.class,
                Text.class,
                PropertyMapMap.createConfiguration(this.state.getElementType()));
        makeMapReduceString(PropertyMap.class);
        this.state.lock();
        return this;
    }

    /**
     * Emit the label of the current edge.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline label() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.assertAtEdge();

        this.property(Tokens.LABEL, String.class);
        return this;
    }

    /**
     * Emit the path taken from start to current element.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline path() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(PathMap.Map.class,
                NullWritable.class,
                Text.class,
                PathMap.createConfiguration(this.state.getElementType()));
        this.state.lock();
        makeMapReduceString(PathMap.class);
        return this;
    }

    /**
     * Order the previous property value results and emit them with another element property value.
     * It is important to emit the previous property with a provided type else it is ordered lexigraphically.
     *
     * @param order      increasing and descending order
     * @param elementKey the key of the element to associate it with
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline order(final TransformPipe.Order order, final String elementKey) {
        this.state.assertNotLocked();
        final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
        if (null != pair) {
            this.compiler.addMapReduce(OrderMapReduce.Map.class,
                    null,
                    OrderMapReduce.Reduce.class,
                    OrderMapReduce.createComparator(order, pair.getB()),
                    pair.getB(),
                    Text.class,
                    Text.class,
                    pair.getB(),
                    OrderMapReduce.createConfiguration(this.state.getElementType(), pair.getA(), pair.getB(), elementKey));
            makeMapReduceString(OrderMapReduce.class, order.name(), elementKey);
        } else {
            throw new IllegalArgumentException("There is no specified property to order on");
        }
        this.state.lock();
        return this;
    }

    /**
     * Order the previous property value results.
     *
     * @param order increasing and descending order
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline order(final TransformPipe.Order order) {
        return this.order(order, Tokens.ID);
    }

    /**
     * Order the previous property value results and emit them with another element property value.
     * It is important to emit the previous property with a provided type else it is ordered lexigraphically.
     *
     * @param order      increasing and descending order
     * @param elementKey the key of the element to associate it with
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline order(final com.tinkerpop.gremlin.Tokens.T order, final String elementKey) {
        return this.order(com.tinkerpop.gremlin.Tokens.mapOrder(order), elementKey);
    }

    /**
     * Order the previous property value results.
     *
     * @param order increasing and descending order
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline order(final com.tinkerpop.gremlin.Tokens.T order) {
        return this.order(com.tinkerpop.gremlin.Tokens.mapOrder(order));
    }


    //////// FILTERS

    /**
     * Emit or deny the current element based upon the provided boolean-based closure.
     *
     * @param closure return true to emit and false to remove.
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline filter(final String closure) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(FilterMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                FilterMap.createConfiguration(this.state.getElementType(), this.validateClosure(closure)));
        makeMapReduceString(FilterMap.class);
        return this;
    }

    /**
     * Emit the current element if it has a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline has(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.has(key, convert(compare), values);
    }

    /**
     * Emit the current element if it does not have a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator (will be not'd)
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline hasNot(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.hasNot(key, convert(compare), values);
    }

    /**
     * Emit the current element if it has a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline has(final String key, final Compare compare, final Object... values) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(PropertyFilterMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                PropertyFilterMap.createConfiguration(this.state.getElementType(), key, compare, values));
        makeMapReduceString(PropertyFilterMap.class, compare.name(), Arrays.asList(values));
        return this;
    }

    /**
     * Emit the current element if it does not have a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator (will be not'd)
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline hasNot(final String key, final Compare compare, final Object... values) {
        return this.has(key, compare.opposite(), values);
    }

    /**
     * Emit the current element it has a property value equal to the provided values.
     *
     * @param key    the property key of the element
     * @param values the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline has(final String key, final Object... values) {
        return (values.length == 0) ? this.has(key, Compare.NOT_EQUAL, new Object[]{null}) : this.has(key, Compare.EQUAL, values);
    }

    /**
     * Emit the current element it does not have a property value equal to the provided values.
     *
     * @param key    the property key of the element
     * @param values the values to compare against where only one needs to succeed (or'd)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline hasNot(final String key, final Object... values) {
        return (values.length == 0) ? this.has(key, Compare.EQUAL, new Object[]{null}) : this.has(key, Compare.NOT_EQUAL, values);
    }

    /**
     * Emit the current element it has a property value equal within the provided range.
     *
     * @param key        the property key of the element
     * @param startValue the start of the range (inclusive)
     * @param endValue   the end of the range (exclusive)
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline interval(final String key, final Object startValue, final Object endValue) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(IntervalFilterMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                IntervalFilterMap.createConfiguration(this.state.getElementType(), key, startValue, endValue));
        makeMapReduceString(IntervalFilterMap.class, key, startValue, endValue);
        return this;
    }

    /**
     * Remove any duplicate traversers at a single element.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline dedup() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(DuplicateFilterMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                DuplicateFilterMap.createConfiguration(this.state.getElementType()));
        makeMapReduceString(DuplicateFilterMap.class);
        return this;
    }

    /**
     * Go back to an element a named step ago.
     * Currently only backing up to vertices is supported.
     *
     * @param step the name of the step to back up to
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline back(final String step) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMapReduce(BackFilterMapReduce.Map.class,
                BackFilterMapReduce.Combiner.class,
                BackFilterMapReduce.Reduce.class,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                BackFilterMapReduce.createConfiguration(this.state.getElementType(), this.state.getStep(step)));
        makeMapReduceString(BackFilterMapReduce.class, step);
        return this;
    }

    /*public HadoopPipeline back(final int numberOfSteps) {
        this.state.assertNotLocked();
        this.compiler.backFilterMapReduce(this.state.getElementType(), this.state.getStep() - numberOfSteps);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(BackFilterMapReduce.class, numberOfSteps);
        return this;
    }*/

    /**
     * Emit the element only if it was arrived at via a path that does not have cycles in it.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline simplePath() {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(CyclicPathFilterMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                CyclicPathFilterMap.createConfiguration(this.state.getElementType()));
        makeMapReduceString(CyclicPathFilterMap.class);
        return this;
    }

    //////// SIDEEFFECTS

    /**
     * Emit the element, but compute some sideeffect in the process.
     * For example, mutate the properties of the element.
     *
     * @param closure the sideeffect closure whose results are ignored.
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline sideEffect(final String closure) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.compiler.addMap(SideEffectMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                SideEffectMap.createConfiguration(this.state.getElementType(), this.validateClosure(closure)));

        makeMapReduceString(SideEffectMap.class);
        return this;
    }

    /**
     * Name a step in order to reference it later in the expression.
     *
     * @param name the string representation of the name
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline as(final String name) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        this.state.addStep(name);

        final String string = "As(" + name + "," + this.stringRepresentation.get(this.state.getStep(name)) + ")";
        this.stringRepresentation.set(this.state.getStep(name), string);
        return this;
    }

    /**
     * Have the elements for the named step previous project an edge to the current vertex with provided label.
     * If a merge weight key is provided, then count the number of duplicate edges between the same two vertices and add a weight.
     * No weight key is specified by "_" and then all duplicates are merged, but no weight is added to the resultant edge.
     *
     * @param step           the name of the step where the source vertices were
     * @param label          the label of the edge to project
     * @param mergeWeightKey the property key to use for weight
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline linkIn(final String label, final String step, final String mergeWeightKey) {
        return this.link(IN, label, step, mergeWeightKey);
    }

    /**
     * Have the elements for the named step previous project an edge to the current vertex with provided label.
     *
     * @param step  the name of the step where the source vertices were
     * @param label the label of the edge to project
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline linkIn(final String label, final String step) {
        return this.link(IN, label, step, null);
    }

    /**
     * Have the elements for the named step previous project an edge from the current vertex with provided label.
     * If a merge weight key is provided, then count the number of duplicate edges between the same two vertices and add a weight.
     * No weight key is specified by "_" and then all duplicates are merged, but no weight is added to the resultant edge.
     *
     * @param step           the name of the step where the source vertices were
     * @param label          the label of the edge to project
     * @param mergeWeightKey the property key to use for weight
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline linkOut(final String label, final String step, final String mergeWeightKey) {
        return link(OUT, label, step, mergeWeightKey);
    }

    /**
     * Have the elements for the named step previous project an edge from the current vertex with provided label.
     *
     * @param step  the name of the step where the source vertices were
     * @param label the label of the edge to project
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline linkOut(final String label, final String step) {
        return this.link(OUT, label, step, null);
    }

    private HadoopPipeline link(final Direction direction, final String label, final String step, final String mergeWeightKey) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        Preconditions.checkNotNull(direction);

        this.compiler.addMapReduce(LinkMapReduce.Map.class,
                LinkMapReduce.Combiner.class,
                LinkMapReduce.Reduce.class,
                null,
                LongWritable.class,
                Holder.class,
                NullWritable.class,
                FaunusVertex.class,
                LinkMapReduce.createConfiguration(direction, label, this.state.getStep(step), mergeWeightKey));

        log.debug("Added {} job with direction {}, label {}, step {}, merge weight key {}", LinkMapReduce.class.getSimpleName(), direction, label, step, mergeWeightKey);

        if (null != mergeWeightKey)
            makeMapReduceString(LinkMapReduce.class, direction.name(), label, step, mergeWeightKey);
        else
            makeMapReduceString(LinkMapReduce.class, direction.name(), label, step);
        return this;
    }

    /**
     * Count the number of times the previous element (or property) has been traversed to.
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended HadoopPipeline.
     */
    public HadoopPipeline groupCount() {
        this.state.assertNotLocked();
        final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
        if (null == pair) {
            return this.groupCount(null, null);
        } else {
            this.compiler.addMapReduce(ValueGroupCountMapReduce.Map.class,
                    ValueGroupCountMapReduce.Combiner.class,
                    ValueGroupCountMapReduce.Reduce.class,
                    pair.getB(),
                    LongWritable.class,
                    pair.getB(),
                    LongWritable.class,
                    ValueGroupCountMapReduce.createConfiguration(this.state.getElementType(), pair.getA(), pair.getB()));
            makeMapReduceString(ValueGroupCountMapReduce.class, pair.getA());
        }
        return this;
    }

    /**
     * Apply the provided closure to the incoming element to determine the grouping key.
     * The value of the count is incremented by 1
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended HadoopPipeline.
     */
    public HadoopPipeline groupCount(final String keyClosure) {
        return this.groupCount(keyClosure, null);
    }

    /**
     * Apply the provided closure to the incoming element to determine the grouping key.
     * Then apply the value closure to the current element to determine the count increment.
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended HadoopPipeline.
     */
    public HadoopPipeline groupCount(final String keyClosure, final String valueClosure) {
        this.state.assertNotLocked();


        this.compiler.addMapReduce(GroupCountMapReduce.Map.class,
                GroupCountMapReduce.Combiner.class,
                GroupCountMapReduce.Reduce.class,
                Text.class,
                LongWritable.class,
                Text.class,
                LongWritable.class,
                GroupCountMapReduce.createConfiguration(this.state.getElementType(),
                        this.validateClosure(keyClosure), this.validateClosure(valueClosure)));

        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }


    private HadoopPipeline commit(final Tokens.Action action) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();

        if (this.state.atVertex()) {
            this.compiler.addMapReduce(CommitVerticesMapReduce.Map.class,
                    CommitVerticesMapReduce.Combiner.class,
                    CommitVerticesMapReduce.Reduce.class,
                    null,
                    LongWritable.class,
                    Holder.class,
                    NullWritable.class,
                    FaunusVertex.class,
                    CommitVerticesMapReduce.createConfiguration(action));
            makeMapReduceString(CommitVerticesMapReduce.class, action.name());
        } else {
            this.compiler.addMap(CommitEdgesMap.Map.class,
                    NullWritable.class,
                    FaunusVertex.class,
                    CommitEdgesMap.createConfiguration(action));
            makeMapReduceString(CommitEdgesMap.class, action.name());
        }
        return this;
    }

    /**
     * Drop all the elements of respective type at the current step. Keep all others.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline drop() {
        return this.commit(Tokens.Action.DROP);
    }

    /**
     * Keep all the elements of the respetive type at the current step. Drop all others.
     *
     * @return the extended HadoopPipeline
     */
    public HadoopPipeline keep() {
        return this.commit(Tokens.Action.KEEP);
    }

    public HadoopPipeline script(final String scriptUri, final String... args) {
        this.state.assertNotLocked();
        this.state.assertNoProperty();
        this.state.assertAtVertex();

        this.compiler.addMap(ScriptMap.Map.class,
                NullWritable.class,
                FaunusVertex.class,
                ScriptMap.createConfiguration(scriptUri, args));
        makeMapReduceString(CommitEdgesMap.class, scriptUri);
        // this.state.lock();
        return this;
    }

    /////////////// UTILITIES

    /**
     * Count the number of traversers currently in the graph
     *
     * @return the count
     */
    public HadoopPipeline count() {
        this.state.assertNotLocked();
        this.compiler.addMapReduce(CountMapReduce.Map.class,
                CountMapReduce.Combiner.class,
                CountMapReduce.Reduce.class,
                NullWritable.class,
                LongWritable.class,
                NullWritable.class,
                LongWritable.class,
                CountMapReduce.createConfiguration(this.state.getElementType()));
        makeMapReduceString(CountMapReduce.class);
        this.state.lock();
        return this;
    }

    public String toString() {
        return this.stringRepresentation.toString();
    }

    private HadoopPipeline done() {
        if (!this.state.isLocked()) {
            final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
            if (null != pair) {
                this.compiler.addMap(PropertyMap.Map.class,
                        LongWritable.class,
                        pair.getB(),
                        PropertyMap.createConfiguration(this.state.getElementType(), pair.getA(), pair.getB()));
                makeMapReduceString(PropertyMap.class, pair.getA());
                this.state.lock();
            }
        }
        return this;
    }

    /**
     * Submit the HadoopPipeline to the Hadoop cluster.
     *
     * @throws Exception
     */
    public void submit() throws Exception {
        submit(Tokens.EMPTY_STRING, false);
    }

    /**
     * Submit the HadoopPipeline to the Hadoop cluster and ensure that a header is emitted in the logs.
     *
     * @param script     the Gremlin script
     * @param showHeader the Titan/Hadoop header
     * @throws Exception
     */
    public void submit(final String script, final Boolean showHeader) throws Exception {
        this.done();
        if (MapReduceFormat.class.isAssignableFrom(this.graph.getGraphOutputFormat())) {
            this.state.assertNotLocked();
            ((Class<? extends MapReduceFormat>) this.graph.getGraphOutputFormat()).getConstructor().newInstance().addMapReduceJobs(this.compiler);
        }
        this.compiler.completeSequence();
        ToolRunner.run(this.compiler, new String[]{script, showHeader.toString()});
    }

    /**
     * Get a reference to the graph currently being used in this HadoopPipeline.
     *
     * @return the HadoopGraph
     */
    public HadoopGraph getGraph() {
        return this.graph;
    }

    public HadoopCompiler getCompiler() {
        return this.compiler;
    }

    private String validateClosure(String closure) {
        if (closure == null)
            return null;

        try {
            engine.eval(closure);
            return closure;
        } catch (ScriptException e) {
            closure = closure.trim();
            closure = closure.replaceFirst("\\{", "{it->");
            try {
                engine.eval(closure);
                return closure;
            } catch (ScriptException e1) {
            }
            throw new IllegalArgumentException("The provided closure does not compile: " + e.getMessage(), e);
        }
    }

    private void makeMapReduceString(final Class klass, final Object... arguments) {
        String result = klass.getSimpleName();
        if (arguments.length > 0) {
            result = result + "(";
            for (final Object arg : arguments) {
                result = result + arg + ",";
            }
            result = result.substring(0, result.length() - 1) + ")";
        }
        this.stringRepresentation.add(result);
    }

    private Class<? extends WritableComparable> convertJavaToHadoop(final Class klass) {
        if (klass.equals(String.class)) {
            return Text.class;
        } else if (klass.equals(Integer.class)) {
            return IntWritable.class;
        } else if (klass.equals(Double.class)) {
            return DoubleWritable.class;
        } else if (klass.equals(Long.class)) {
            return LongWritable.class;
        } else if (klass.equals(Float.class)) {
            return FloatWritable.class;
        } else if (klass.equals(Boolean.class)) {
            return BooleanWritable.class;
        } else {
            throw new IllegalArgumentException("The provided class is not supported: " + klass.getSimpleName());
        }
    }
}
