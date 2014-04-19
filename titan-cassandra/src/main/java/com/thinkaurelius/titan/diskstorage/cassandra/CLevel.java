package com.thinkaurelius.titan.diskstorage.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Preconditions;

/**
 * This enum unites different libraries' consistency level enums, streamlining
 * configuration and processing in {@link AbstractCassandraStoreManager}.
 *
 */
public enum CLevel implements CLevelInterface { // One ring to rule them all
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM;

    private final org.apache.cassandra.db.ConsistencyLevel db;
    private final org.apache.cassandra.thrift.ConsistencyLevel thrift;
    private final com.netflix.astyanax.model.ConsistencyLevel astyanax;
    private final com.datastax.driver.core.ConsistencyLevel cql;

    private CLevel() {
        db = org.apache.cassandra.db.ConsistencyLevel.valueOf(toString());
        thrift = org.apache.cassandra.thrift.ConsistencyLevel.valueOf(toString());
        astyanax = com.netflix.astyanax.model.ConsistencyLevel.valueOf("CL_" + toString());
        cql = toCQL(db);
    }

    @Override
    public org.apache.cassandra.db.ConsistencyLevel getDB() {
        return db;
    }

    @Override
    public org.apache.cassandra.thrift.ConsistencyLevel getThrift() {
        return thrift;
    }

    @Override
    public com.netflix.astyanax.model.ConsistencyLevel getAstyanax() {
        return astyanax;
    }

    @Override
    public com.datastax.driver.core.ConsistencyLevel getCQL() {
        return cql;
    }

    public static CLevel parse(String value) {
        Preconditions.checkArgument(value != null && !value.isEmpty());
        value = value.trim();
        if (value.equals("1")) return ONE;
        else if (value.equals("2")) return TWO;
        else if (value.equals("3")) return THREE;
        else {
            for (CLevel c : values()) {
                if (c.toString().equalsIgnoreCase(value) ||
                    ("CL_" + c.toString()).equalsIgnoreCase(value)) return c;
            }
        }
        throw new IllegalArgumentException("Unrecognized cassandra consistency level: " + value);
    }

    public static com.datastax.driver.core.ConsistencyLevel toCQL(org.apache.cassandra.db.ConsistencyLevel dbCL) {
        switch (dbCL) {
            case ONE:
                return ConsistencyLevel.ONE;
            case TWO:
                return ConsistencyLevel.TWO;
            case THREE:
                return ConsistencyLevel.THREE;
            case QUORUM:
                return ConsistencyLevel.QUORUM;
            case LOCAL_QUORUM:
                return ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM:
                return ConsistencyLevel.EACH_QUORUM;
            case ALL:
                return ConsistencyLevel.ALL;
        }

        return ConsistencyLevel.ONE;
    }
}
