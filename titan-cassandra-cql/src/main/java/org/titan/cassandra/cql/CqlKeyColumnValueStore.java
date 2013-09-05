package org.titan.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ByteBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class CqlKeyColumnValueStore implements KeyColumnValueStore {
	private static final Logger log = LoggerFactory
			.getLogger(CqlKeyColumnValueStore.class);

	private final Session session;
	private String name;

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
	public boolean containsKey(StaticBuffer key, StoreTransaction txh)
			throws StorageException {

		BoundStatement boundStmt = new BoundStatement(readKeyStatement);
		boundStmt.setConsistencyLevel(CqlTransaction.getTx(txh)
				.getReadConsistency().getCqlConsistency());

		boundStmt.setBytes("rowKey", key.asByteBuffer());

		ResultSet rs = session.execute(boundStmt);
		if (rs.iterator().hasNext()) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh)
			throws StorageException {
		int limit = Integer.MAX_VALUE - 1;
		if (query.hasLimit())
			limit = query.getLimit();

		log.info("Table: {}  Key: {}", getName(), query.getKey());

		BoundStatement boundStmt = new BoundStatement(readKeyValueStatement);
		boundStmt.setConsistencyLevel(CqlTransaction.getTx(txh)
				.getReadConsistency().getCqlConsistency());
		boundStmt.setBytes("rowKey", query.getKey().asByteBuffer());
		boundStmt.setBytes(1, query.getSliceStart().asByteBuffer());
		boundStmt.setBytes(2, query.getSliceEnd().asByteBuffer());

		List<Entry> entries = new ArrayList<Entry>(1);
		// order of the slices matter.
		if (query.getSliceStart().compareTo(query.getSliceEnd()) == -1) {
			ResultSet rs = session.execute(boundStmt);
			int counter = 0;
			while (rs.iterator().hasNext()) {
				Row row = rs.iterator().next();
				ByteBuffer column = row.getBytes(0);
				ByteBuffer value = row.getBytes(1);
				ByteBufferEntry entry = new ByteBufferEntry(column, value);
				entries.add(entry);
				if (++counter > (limit - 1))
					break;
			}
			log.info("Counter: {}", counter);
		}
		log.info("Entries: {}", entries.size());

		return entries;
	}

	@Override
	public void mutate(StaticBuffer key, List<Entry> additions,
			List<StaticBuffer> deletions, StoreTransaction txh)
			throws StorageException {
		log.info("Table: {} Key: {} Addtions: {}, Deletions: {}", new Object[] {
				getName(), key.toString(), additions.size(), deletions.size() });
		if (!deletions.isEmpty()) {
			removeEntries(key, deletions, txh);
		}

		if (!additions.isEmpty()) {
			addEntries(key, additions, txh);
		}

	}

	@Override
	public void acquireLock(StaticBuffer key, StaticBuffer column,
			StaticBuffer expectedValue, StoreTransaction txh)
			throws StorageException {
		throw new UnsupportedOperationException();

	}

	@Override
	public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh)
			throws StorageException {
		StringBuilder sb = new StringBuilder("SELECT rowKey FROM ");
		sb.append(name);
		ResultSet rs = session.execute(sb.toString());
		final Iterator<Row> it = rs.iterator();

		// because of our cql model, we have to select distinct the keys...
		final Set<StaticByteBuffer> seenKeys = new TreeSet<StaticByteBuffer>();
		while (it.hasNext()) {
			StaticByteBuffer key = new StaticByteBuffer(it.next().getBytes(
					"rowKey"));
			seenKeys.add(key);
		}

		final Iterator<StaticByteBuffer> keyIterator = seenKeys.iterator();

		return new RecordIterator<StaticBuffer>() {

			@Override
			public boolean hasNext() throws StorageException {
				return keyIterator.hasNext();
			}

			@Override
			public StaticBuffer next() throws StorageException {
				return keyIterator.next();
			}

			@Override
			public void close() throws StorageException {
				seenKeys.clear();
			}
		};
	}

	@Override
	public StaticBuffer[] getLocalKeyPartition() throws StorageException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void close() throws StorageException {
		session.shutdown();
	}

	private void addEntries(StaticBuffer key, List<Entry> additions,
			StoreTransaction txh) {
		for (Entry entry : additions) {
			BoundStatement boundStmt = new BoundStatement(
					writeKeyValueStatement);
			boundStmt.setConsistencyLevel(CqlTransaction.getTx(txh)
					.getWriteConsistency().getCqlConsistency());
			boundStmt.setBytes("rowKey", key.asByteBuffer());
			boundStmt.setBytes("columnName", entry.getColumn().asByteBuffer());
			boundStmt.setBytes("value", entry.getValue().asByteBuffer());

			session.execute(boundStmt);

			log.info(
					"Add entry: name: {}; rowKey: {}; columnName: {}; value: {}",
					new Object[] { getName(), key.toString(),
							entry.getByteBufferColumn().toString(),
							entry.getByteBufferValue().toString() });

		}

	}

	private void removeEntries(StaticBuffer key, List<StaticBuffer> deletions,
			StoreTransaction txh) {
		// FIXME: can we do it with a batch ?
		for (StaticBuffer entry : deletions) {
			BoundStatement boundStmt = new BoundStatement(
					removeKeyValueStatement);
			boundStmt.setConsistencyLevel(CqlTransaction.getTx(txh)
					.getWriteConsistency().getCqlConsistency());
			boundStmt.setBytes("rowKey", key.asByteBuffer());
			boundStmt.setBytes("columnName", entry.asByteBuffer());
			session.execute(boundStmt);
			log.info(
					"Remove entry: name: {}; rowKey: {}; columnName: {};\n",
					new Object[] { getName(), key.toString(), entry.toString() });

		}

	}

	private PreparedStatement buildReadKeyStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("select rowKey from ").append(name).append(" where rowKey=?");
		return session.prepare(sb.toString());
	}

	private PreparedStatement buildReadKeyRangeStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("select rowKey from ").append(name)
				.append(" where rowKey >= ? and rowKey < ?");
		return session.prepare(sb.toString());
	}

	private PreparedStatement buildReadKeyValueStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("select columnName, value from ")
				.append(name)
				.append(" where rowKey=? and columnName >= ? and columnName < ?");

		return session.prepare(sb.toString());

	}

	private PreparedStatement buildWriteKeyValueStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(name)
				.append(" (rowKey, columnName, value) values (?,?,?)");

		return session.prepare(sb.toString());

	}

	private PreparedStatement buildRemoveKeyValueStatement(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ").append(name)
				.append(" WHERE rowKey=? AND columnName=?");

		return session.prepare(sb.toString());

	}

	@Override
	public List<List<Entry>> getSlice(List<StaticBuffer> keys,
			SliceQuery query, StoreTransaction txh) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh)
			throws StorageException {
		
		return null;
	}

	@Override
	public KeyIterator getKeys(SliceQuery query, StoreTransaction txh)
			throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

}
