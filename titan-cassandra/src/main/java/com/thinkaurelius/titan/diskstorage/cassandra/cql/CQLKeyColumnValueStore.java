package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CQLKeyColumnValueStore implements KeyColumnValueStore {
	private static final Logger logger = LoggerFactory.getLogger(CQLKeyColumnValueStore.class);

	private final Session session;
	private final String name;

	private final PreparedStatement updateStatement;

	CQLKeyColumnValueStore(String name, final Session session) {
        this.name = name;
        this.session = session;
        this.updateStatement = session.prepare("UPDATE " + name + " SET v = ? WHERE key = ? AND c = ?");
	}

	@Override
	public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
		ResultSet rs = session.execute(QueryBuilder.select().column("key").from(name).where(eq("key", key.asByteBuffer())));
        return rs.iterator().hasNext();
	}

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (logger.isDebugEnabled())
            logger.debug("Table: {} Key: {} Addtions: {}, Deletions: {}", getName(), key.toString(), additions.size(), deletions.size());

        if (!deletions.isEmpty()) {
            removeEntries(key, deletions, txh);
        }

        if (!additions.isEmpty()) {
            addEntries(key, additions, txh);
        }
    }

	@Override
	public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws StorageException {
		if (logger.isDebugEnabled())
		    logger.debug("Table: {}  Key: {}", getName(), query.getKey());

        /*
         * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's CQL getSlice() throws InvalidQueryException if columnStart = columnEnd.
		 */
        if (query.getSliceStart().compareTo(query.getSliceEnd()) >= 0) {
            // Check for invalid arguments where columnEnd < columnStart
            if (query.getSliceEnd().compareTo(query.getSliceStart()) < 0) {
                throw new PermanentStorageException("columnStart=" + query.getSliceStart() +
                        " is greater than columnEnd=" + query.getSliceEnd() + ". " +
                        "columnStart must be less than or equal to columnEnd");
            }

            if (query.getSliceStart().length() != 0 && query.getSliceEnd().length() != 0) {
                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                return EntryList.EMPTY_LIST;
            }
        }
        ConsistencyLevel cl = CassandraTransaction.getTx(txh).getReadConsistencyLevel().getCQL();

        ByteBuffer key = query.getKey().asByteBuffer();
        ByteBuffer start  = query.getSliceStart().asByteBuffer();
        ByteBuffer finish = query.getSliceEnd().asByteBuffer();

        Select select = QueryBuilder.select().column("c").column("v").from(name);
        {
            select.where(eq("key", key)).and(gte("c", start)).and(lt("c", finish));
            select.setConsistencyLevel(cl);

            if (query.hasLimit())
                select.limit(query.getLimit());
        }

        List<Row> rows = session.execute(select).all();

        if (rows == null || rows.size() == 0)
            return EntryList.EMPTY_LIST;

        return CassandraHelper.makeEntryList(rows, CassandraCQLGetter.INSTANCE, query.getSliceEnd(), query.getLimit());
	}

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, EntryList> result = new HashMap<StaticBuffer, EntryList>();

        for (StaticBuffer key : keys) {
            result.put(key, getSlice(new KeySliceQuery(key, query), txh));
        }

        return result;
    }

    @Override
	public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh)
			throws StorageException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<KeyRange> getLocalKeyPartition() throws StorageException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void close() throws StorageException {
		session.close();
	}

	private void addEntries(StaticBuffer key, List<Entry> additions, StoreTransaction txh) {
        // TODO: move to batch once on 2.0.x version
        //BatchStatement batch = new BatchStatement();
        //batch.setConsistencyLevel(CassandraTransaction.getTx(txh).getReadConsistencyLevel().getCQL());

        ConsistencyLevel cl = CassandraTransaction.getTx(txh).getReadConsistencyLevel().getCQL();
		for (Entry entry : additions) {
            ByteBuffer col = entry.getColumn().asByteBuffer();
            ByteBuffer val = entry.getValue().asByteBuffer();

            session.execute(updateStatement.bind(val, key.asByteBuffer(), col).setConsistencyLevel(cl));

            if (logger.isDebugEnabled())
			    logger.debug("Add entry: name: {}; rowKey: {}; columnName: {}; value: {}",
                        getName(), key.toString(), col.toString(), val.toString());

		}
	}

	private void removeEntries(StaticBuffer key, List<StaticBuffer> deletions, StoreTransaction txh) {
        // TODO: move to batch once on 2.0.x version
        //BatchStatement batch = new BatchStatement();
        //batch.setConsistencyLevel(CassandraTransaction.getTx(txh).getReadConsistencyLevel().getCQL());

        ConsistencyLevel cl = CassandraTransaction.getTx(txh).getReadConsistencyLevel().getCQL();
        for (StaticBuffer entry : deletions) {
            session.execute(QueryBuilder.delete().all().from(name)
                                        .where(eq("key", key.asByteBuffer())).and(eq("c", entry.asByteBuffer()))
                                        .setConsistencyLevel(cl));

            if (logger.isDebugEnabled())
                logger.debug("Remove entry: name: {}; rowKey: {}; columnName: {}",
                        getName(), key.toString(), entry.toString());
		}
	}

    @Override
	public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
	}

	@Override
	public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
		throw new UnsupportedOperationException();
	}

    private static enum CassandraCQLGetter implements StaticArrayEntry.GetColVal<Row, ByteBuffer> {
        INSTANCE;

        @Override
        public ByteBuffer getColumn(Row element) {
            return ByteBufferUtil.clone(element.getBytes("c"));
        }

        @Override
        public ByteBuffer getValue(Row element) {
            return ByteBufferUtil.clone(element.getBytes("v"));
        }
    }
}
