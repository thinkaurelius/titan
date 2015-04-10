package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex,E> {

    public TitanVertexStep(VertexStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(), originalStep.getEdgeLabels());
        if (originalStep.getLabel().isPresent())
            this.setLabel(originalStep.getLabel().get());
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    private boolean initialized = false;
    private boolean useMultiQuery = false;
    private Map<TitanVertex, Iterable<? extends TitanElement>> multiQueryResults = null;

    void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    public<Q extends BaseVertexQuery> Q makeQuery(Q query) {
        query.labels(getEdgeLabels());
        query.direction(getDirection());
        for (HasContainer condition : hasContainers) {
            if (condition.predicate instanceof Contains && condition.value==null) {
                if (condition.predicate==Contains.within) query.has(condition.key);
                else query.hasNot(condition.key);
            } else {
                query.has(condition.key, TitanPredicate.Converter.convert(condition.predicate), condition.value);
            }
        }
        for (OrderEntry order : orders) query.orderBy(order.key,order.order);
        if (limit !=BaseQuery.NO_LIMIT) query.limit(limit);
        return query;
    }

    private void initialize() {
        assert !initialized;
        initialized = true;
        if (useMultiQuery) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            TitanMultiVertexQuery mquery = TitanTraversalUtil.getTx(traversal).multiQuery();
            List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
            starts.forEachRemaining(v -> { vertices.add(v); mquery.addVertex(v.get()); });
            starts.add(vertices.iterator());
            assert vertices.size()>0;
            makeQuery(mquery);

            multiQueryResults = (Vertex.class.isAssignableFrom(getReturnClass())) ? mquery.vertices() : mquery.edges();
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        if (useMultiQuery) {
            assert multiQueryResults!=null;
            return (Iterator<E>)multiQueryResults.get(traverser.get()).iterator();
        } else {
            TitanVertexQuery query = makeQuery((TitanTraversalUtil.getTitanVertex(traverser)).query());
            return (Iterator<E>)((Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices().iterator() : query.edges().iterator());
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public TitanVertexStep<E> clone() {
        final TitanVertexStep<E> clone = (TitanVertexStep<E>) super.clone();
        clone.initialized=false;
        return clone;
    }

    /*
    ===== HOLDER =====
     */

    private final List<HasContainer> hasContainers;
    private int limit = BaseQuery.NO_LIMIT;
    private List<OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key,order));
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return this.limit;
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : TraversalHelper.makeStepString(this, this.hasContainers);
    }

}
