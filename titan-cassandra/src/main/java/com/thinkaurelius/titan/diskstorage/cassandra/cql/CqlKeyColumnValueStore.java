package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

public class CqlKeyColumnValueStore implements KeyColumnValueStore {
	private static final Logger logger = LoggerFactory.getLogger(CqlKeyColumnValueStore.class);

	private final Session session;
	private final String name;

	private final PreparedStatement readKeyStatement;
	private final PreparedStatement readKeyValueStatement;
	private final PreparedStatement writeKeyValueStatement;
	private final PreparedStatement removeKeyValueStatement;

	CqlKeyColumnValueStore(String name, final Session session) {
		this.session = session;
		this.name = name;
		this.readKeyStatement = buildReadKeyStatement(name);
		this.readKeyValueStatement = buildReadKeyValueStatement(name);
		this.writeKeyValueStatement = buildWriteKeyValueStatement(name);
		this.removeKeyValueStatement = buildRemoveKeyValueStatement(name);
	}

	@Override
	public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
		ResultSet rs = session.execute(readKeyStatement.bind(key.asByteBuffer()));
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
	public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
		int limit = Integer.MAX_VALUE - 1;
		if (query.hasLimit())
			limit = query.getLimit();

        if (logger.isDebugEnabled())
		    logger.debug("Table: {}  Key: {}", getName(), query.getKey());

		BoundStatement boundStmt = new BoundStatement(readKeyValueStatement);
		boundStmt.setConsistencyLevel(CqlTransaction.getTx(txh).getReadConsistency().getCqlConsistency());
		boundStmt.setBytes("key", query.getKey().asByteBuffer());
		boundStmt.setBytes(1, query.getSliceStart().asByteBuffer());
		boundStmt.setBytes(2, query.getSliceEnd().asByteBuffer());

		EntryList entries = new StaticArrayEntryList();

		// order of the slices matter.
		if (query.getSliceStart().compareTo(query.getSliceEnd()) == -1) {
			int counter = 0;

            for (Row row : session.execute(boundStmt)) {
                ByteBuffer column = row.getBytes(0);
                ByteBuffer value = row.getBytes(1);
                ByteBufferEntry entry = new ByteBufferEntry(column, value);
                entries.add(entry);

                if (++counter > (limit - 1))
                    break;
            }
		}

		return entries;
	}

	@Override
	public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh)
			throws StorageException {
		throw new UnsupportedOperationException();
	}

    /*
	@Override
	public KeyIterator getKeys(KeyRangeQuery keys, StoreTransaction txh) throws StorageException {
		StringBuilder sb = new StringBuilder("SELECT key FROM ").append(name);
		ResultSet rs = session.execute(sb.toString());
		final Iterator<Row> it = rs.iterator();

		// because of our cql model, we have to select distinct the keys...
		final Set<StaticByteBuffer> seenKeys = new TreeSet<StaticByteBuffer>();
		while (it.hasNext()) {
			StaticByteBuffer key = new StaticByteBuffer(it.next().getBytes("key"));
			seenKeys.add(key);
		}

		final Iterator<StaticByteBuffer> keyIterator = seenKeys.iterator();

		return new RecordIterator<StaticBuffer>() {

			@Override
			public boolean hasNext() {
				return keyIterator.hasNext();
			}

			@Override
			public StaticBuffer next() {
				return keyIterator.next();
			}

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
			public void close() {
				seenKeys.clear();
			}
		};
	}
    */

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
        //batch.setConsistencyLevel(CqlTransaction.getTx(txh).getWriteConsistency().getCqlConsistency());

        ConsistencyLevel cl = CqlTransaction.getTx(txh).getWriteConsistency().getCqlConsistency();
		for (Entry entry : additions) {
            ByteBuffer col = entry.getColumn().asByteBuffer();
            ByteBuffer val = entry.getValue().asByteBuffer();

            session.execute(writeKeyValueStatement.bind(val, col, key.asByteBuffer()).setConsistencyLevel(cl));

            if (logger.isDebugEnabled())
			    logger.debug("Add entry: name: {}; rowKey: {}; columnName: {}; value: {}",
                        getName(), key.toString(), col.toString(), val.toString());

		}
	}

	private void removeEntries(StaticBuffer key, List<StaticBuffer> deletions, StoreTransaction txh) {
        // TODO: move to batch once on 2.0.x version
        //BatchStatement batch = new BatchStatement();
        //batch.setConsistencyLevel(CqlTransaction.getTx(txh).getWriteConsistency().getCqlConsistency());

        ConsistencyLevel cl = CqlTransaction.getTx(txh).getWriteConsistency().getCqlConsistency();
        for (StaticBuffer entry : deletions) {
            session.execute(removeKeyValueStatement.bind(key.asByteBuffer(), entry.asByteBuffer()).setConsistencyLevel(cl));
            if (logger.isDebugEnabled())
                logger.info("Remove entry: name: {}; rowKey: {}; columnName: {};\n",
                            getName(), key.toString(), entry.toString());

		}
	}

	private PreparedStatement buildReadKeyStatement(String name) {
		return session.prepare("SELECT key FROM " + name + " WHERE key = ?");
	}

    /*
	private PreparedStatement buildReadKeyRangeStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("select rowKey from ").append(name)
				.append(" where rowKey >= ? and rowKey < ?");
		return session.prepare(sb.toString());
	}
    */
	private PreparedStatement buildReadKeyValueStatement(String name) {
		return session.prepare("SELECT c, v FROM " + name + " WHERE key = ? AND c >= ? AND c < ?");

	}

	private PreparedStatement buildWriteKeyValueStatement(String name) {
		return session.prepare("UPDATE " + name + " SET v = ? WHERE key = ? AND c = ?");

	}

	private PreparedStatement buildRemoveKeyValueStatement(String name) {
		return session.prepare("DELETE FROM " + name + " WHERE key = ? AND c = ?");
	}

	@Override
	public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
		throw new UnsupportedOperationException();
	}

    @Override
	public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
	}

	@Override
	public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
		throw new UnsupportedOperationException();
	}

    private static class BufferEntryList extends ArrayList<Entry> implements EntryList {

        @Override
        public Iterator<Entry> reuseIterator() {
            return null;
        }

        @Override
        public int getByteSize() {
            return 0;
        }
    }

}
