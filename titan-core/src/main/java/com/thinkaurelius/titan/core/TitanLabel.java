
package com.thinkaurelius.titan.core;

/**
 * TitanLabel is an extension of {@link TitanType} for edges.
 * 
 * In addition to {@link TitanType}, TitanLabel defines the directionality of edges:
 * <ul>
 *     <li><strong>Directed:</strong> An edge is directed if the order or position of its vertices matters.
 *     For directed edges, the outgoing vertex is considered the <i>start</i> and the incoming vertex the <i>end</i>.
 *     By default, labels and therefore edges are directed. <i>Father</i> is an example of a directed label.</li>
 *     <li><strong>Undirected:</strong> An edge is undirected if the order or position of its vertices is irrelevant.
 *     In other words, there is no directionality expressed by the edge. <i>Friend</i> is an example of a undirected label. </li>
 *     <li><strong>Unidirected:</strong> An edge is unidirected if it is directed but only pointing in one direction.
 *     This means, the edge can only be traversed in the outgoing direction. As an example, a hyperlink is a unidirected
 *     edge. Unidirected edges can be stored more efficiently.</li>
 * </ul>
 * 
 * @see TitanType
 *
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com) 
 *
 */
public interface TitanLabel extends TitanType {

    /**
     * Checks whether this labels is defined as directed.
     * @return true, if this label is directed, else false.
     */
    public boolean isDirected();

    /**
     * Checks whether this labels is defined as undirected.
     * @return true, if this label is undirected, else false.
     */
    public boolean isUndirected();

    /**
     * Checks whether this labels is defined as unidirected.
     * @return true, if this label is unidirected, else false.
     */
    public boolean isUnidirected();

}
