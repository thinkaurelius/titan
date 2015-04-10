package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * A TitanVertexQuery is a VertexQuery executed for a single vertex.
 * <p />
 * Calling {@link com.thinkaurelius.titan.core.TitanVertex#query()} builds such a query against the vertex
 * this method is called on. This query builder provides the methods to specify which indicent edges or
 * properties to query for.
 *
 *
 * @see BaseVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanVertexQuery extends BaseVertexQuery<TitanVertexQuery> {

   /* ---------------------------------------------------------------
    * Query Specification (overwrite to merge BaseVertexQuery with Blueprint's VertexQuery)
    * ---------------------------------------------------------------
    */

    @Override
    public TitanVertexQuery adjacent(Vertex vertex);

    @Override
    public TitanVertexQuery types(String... type);

    @Override
    public TitanVertexQuery types(RelationType... type);

    @Override
    public TitanVertexQuery labels(String... labels);

    @Override
    public TitanVertexQuery keys(String... keys);

    @Override
    public TitanVertexQuery direction(Direction d);

    @Override
    public TitanVertexQuery has(String type, Object value);

    @Override
    public TitanVertexQuery has(String key);

    @Override
    public TitanVertexQuery hasNot(String key);

    @Override
    public TitanVertexQuery hasNot(String key, Object value);

    @Override
    public TitanVertexQuery has(String key, TitanPredicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> TitanVertexQuery interval(String key, T start, T end);

    @Override
    public TitanVertexQuery limit(int limit);

    @Override
    public TitanVertexQuery orderBy(String key, Order order);


    /* ---------------------------------------------------------------
    * Query execution
    * ---------------------------------------------------------------
    */

    /**
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    public Iterable<TitanEdge> edges();


    public Iterable<TitanVertex> vertices();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    public Iterable<TitanVertexProperty> properties();

    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    public Iterable<TitanRelation> relations();

    /**
     * Returns the number of edges that match this query
     *
     * @return Number of edges that match this query
     */
    public long count();

    /**
     * Returns the number of properties that match this query
     *
     * @return Number of properties that match this query
     */
    public long propertyCount();

    /**
     * Retrieves all vertices connected to this query's base vertex by edges
     * matching the conditions defined in this query.
     * <p/>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's base vertex by matching edges
     */
    public VertexList vertexIds();


}
