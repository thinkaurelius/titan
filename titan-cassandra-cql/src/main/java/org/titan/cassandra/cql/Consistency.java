package org.titan.cassandra.cql;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Preconditions;

/**
 * Map CQL consistency levels to Titan.
 * 
 * @author gciuloaica
 * 
 */
public enum Consistency {
	ONE, TWO, THREE, ANY, ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM;

	public static Consistency parse(String value) {
		Preconditions.checkArgument(value != null && !value.isEmpty());
		value = value.trim();
		if (value.equals("1"))
			return ONE;
		else if (value.equals("2"))
			return TWO;
		else if (value.equals("3"))
			return THREE;
		else {
			for (Consistency c : values()) {
				if (c.toString().equalsIgnoreCase(value))
					return c;
			}
		}
		throw new IllegalArgumentException(
				"Unrecognized cassandra consistency level: " + value);
	}

	public ConsistencyLevel getCqlConsistency() {
		switch (this) {
		case ONE:
			return ConsistencyLevel.ONE;
		case TWO:
			return ConsistencyLevel.TWO;
		case THREE:
			return ConsistencyLevel.THREE;
		case ALL:
			return ConsistencyLevel.ALL;
		case ANY:
			return ConsistencyLevel.ANY;
		case QUORUM:
			return ConsistencyLevel.QUORUM;
		case LOCAL_QUORUM:
			return ConsistencyLevel.LOCAL_QUORUM;
		case EACH_QUORUM:
			return ConsistencyLevel.EACH_QUORUM;
		default:
			throw new IllegalArgumentException(
					"Unrecognized consistency level: " + this);
		}
	}

}
