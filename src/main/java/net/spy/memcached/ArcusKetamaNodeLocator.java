/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.MigrationMode;
import net.spy.memcached.util.ArcusKetamaNodeLocatorConfiguration;

public class ArcusKetamaNodeLocator extends SpyObject implements NodeLocator {

	TreeMap<Long, MemcachedNode> ketamaNodes;
	Collection<MemcachedNode> allNodes;

	/* ENABLE_MIGRATION if */
	TreeMap<Long, MemcachedNode> migrationKetamaNodes;
	Collection<MemcachedNode> allMigrationNodes;
	/* ENABLE_MIGRATION end */

	HashAlgorithm hashAlg;
	ArcusKetamaNodeLocatorConfiguration config;

	Lock lock = new ReentrantLock();

	public ArcusKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
		this(nodes, alg, new ArcusKetamaNodeLocatorConfiguration());
	}

	public ArcusKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
			ArcusKetamaNodeLocatorConfiguration conf) {
		super();
		allNodes = nodes;
		hashAlg = alg;
		ketamaNodes = new TreeMap<Long, MemcachedNode>();
		config = conf;

		migrationKetamaNodes = new TreeMap<Long, MemcachedNode>();
		allMigrationNodes = new ArrayList<MemcachedNode>();

		int numReps = config.getNodeRepetitions();
		for (MemcachedNode node : nodes) {
			// Ketama does some special work with md5 where it reuses chunks.
			if (alg == HashAlgorithm.KETAMA_HASH) {
				updateHash(node, false);
			} else {
				for (int i = 0; i < numReps; i++) {
					ketamaNodes.put(
							hashAlg.hash(config.getKeyForNode(node, i)), node);
				}
			}
		}
		assert ketamaNodes.size() == numReps * nodes.size();
	}

	private ArcusKetamaNodeLocator(TreeMap<Long, MemcachedNode> smn,
			Collection<MemcachedNode> an, HashAlgorithm alg,
			ArcusKetamaNodeLocatorConfiguration conf) {
		super();
		ketamaNodes = smn;
		allNodes = an;
		hashAlg = alg;
		config = conf;
	}

	public Collection<MemcachedNode> getAll() {
		return allNodes;
	}

	public Collection<MemcachedNode> getAllMigrationNodes() {
		return allMigrationNodes;
	}

	public MemcachedNode getPrimary(final String k) {
		MemcachedNode rv = getNodeForKey(hashAlg.hash(k));
		assert rv != null : "Found no node for key " + k;
		return rv;
	}

	long getMaxKey() {
		return ketamaNodes.lastKey();
	}

	MemcachedNode getNodeForKey(long hash) {
		MemcachedNode rv;

		lock.lock();
		try {
			if (!ketamaNodes.containsKey(hash)) {
				Long nodeHash = ketamaNodes.ceilingKey(hash);
				if (nodeHash == null) {
					hash = ketamaNodes.firstKey();
				} else {
					hash = nodeHash.longValue();
				}
				// Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
				// in a lot of places, so I'm doing this myself.
				/*
				SortedMap<Long, MemcachedNode> tailMap = ketamaNodes.tailMap(hash);
				if (tailMap.isEmpty()) {
					hash = ketamaNodes.firstKey();
				} else {
					hash = tailMap.firstKey();
				}
				*/
			}
			rv = ketamaNodes.get(hash);
		} catch (RuntimeException e) {
			throw e;
		} finally {
			lock.unlock();
		}
		return rv;
	}

	public Iterator<MemcachedNode> getSequence(String k) {
		return new KetamaIterator(k, allNodes.size());
	}

	public NodeLocator getReadonlyCopy() {
		TreeMap<Long, MemcachedNode> smn = new TreeMap<Long, MemcachedNode>(
				ketamaNodes);
		Collection<MemcachedNode> an = new ArrayList<MemcachedNode>(
				allNodes.size());

		lock.lock();
		try {
			// Rewrite the values a copy of the map.
			for (Map.Entry<Long, MemcachedNode> me : smn.entrySet()) {
				me.setValue(new MemcachedNodeROImpl(me.getValue()));
			}
			// Copy the allNodes collection.
			for (MemcachedNode n : allNodes) {
				an.add(new MemcachedNodeROImpl(n));
			}
		} catch (RuntimeException e) {
			throw e;
		} finally {
			lock.unlock();
		}

		return new ArcusKetamaNodeLocator(smn, an, hashAlg, config);
	}

	public void update(Collection<MemcachedNode> toAttach,
			Collection<MemcachedNode> toDelete) {
		lock.lock();
		try {
			// Add memcached nodes.
			for (MemcachedNode node : toAttach) {
				allNodes.add(node);
				updateHash(node, false);
			}

			// Remove memcached nodes.
			for (MemcachedNode node : toDelete) {
				allNodes.remove(node);
				updateHash(node, true);

				try {
					node.getSk().attach(null);
					node.shutdown();
				} catch (IOException e) {
					getLogger().error(
							"Failed to shutdown the node : " + node.toString());
					node.setSk(null);
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} finally {
			lock.unlock();
		}
	}

	private void updateHash(MemcachedNode node, boolean remove) {
		if (!remove) {
			config.insertNode(node);
		}
		
		// Ketama does some special work with md5 where it reuses chunks.
		for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
			
			byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
			for (int h = 0; h < 4; h++) {
				Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
						| ((long) (digest[2 + h * 4] & 0xFF) << 16)
						| ((long) (digest[1 + h * 4] & 0xFF) << 8)
						| (digest[h * 4] & 0xFF);

				if (remove) {
					ketamaNodes.remove(k);
				} else {
					ketamaNodes.put(k, node);
				}
			}
		}
		
		if (remove) {
			config.removeNode(node);
		}
	}

	private void updateMigrationHash(MemcachedNode node) {

		// Ketama does some special work with md5 where it reuses chunks.
		for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {

			byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
			for (int h = 0; h < 4; h++) {
				Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
						| ((long) (digest[2 + h * 4] & 0xFF) << 16)
						| ((long) (digest[1 + h * 4] & 0xFF) << 8)
						| (digest[h * 4] & 0xFF);

				migrationKetamaNodes.put(k, node);
			}
		}
	}

	public void updateMigration(Collection<MemcachedNode> toAttach, MigrationMode mode) {
		switch (mode) {
		case Join:
			for(MemcachedNode node : toAttach) {
				allMigrationNodes.add(node);
				updateMigrationHash(node);
			}
			break;
		case Leave:
			for(MemcachedNode node : toAttach) {
				allMigrationNodes.add(node);
				updateMigrationHash(node);
			}
			break;
		}
	}

	public void migrateHash(MemcachedNode node, String response, MigrationMode mode) {
		long[] hpoint = new long[2];

		String[] splitedResponse = response.split(" ");

		if (splitedResponse[0].equals("NOT_MY_KEY")) {
			// It is not NOT_MY_KEY
			return;
		}

		/* get key set */
		hpoint[0] = Long.valueOf(splitedResponse[1]);
		hpoint[1] = Long.valueOf(splitedResponse[2]);

		NavigableSet keySet = migrationKetamaNodes.navigableKeySet();
		Set subset = keySet.subSet(migrationKetamaNodes.ceilingKey(hpoint[0]), migrationKetamaNodes.floorKey(hpoint[1]));
		Long[] keys = (Long [])subset.toArray(new Long[subset.size()]);

		switch (mode) {
		case Join:
			/* join all or partial hash slices from migrationKetamaNodes to ketamaNodes */

			//TODO : parse the response string and get the range of hash point

			if(migrationKetamaNodes != null && !migrationKetamaNodes.isEmpty()) {

				lock.lock();
				try {
					for (int i = 0; i < keys.length; i++) {
						MemcachedNode removed = migrationKetamaNodes.remove(keys[i]);

						if (removed != null) {
							ketamaNodes.put(keys[i], removed);
						} else {
							// The node does not exist.
						}
					}
				} catch (IndexOutOfBoundsException e) {
					e.printStackTrace(); // optional
				} finally {
					lock.unlock();
				}
			} else {
				// ERROR : Migration Hash Ring is null or empty
			}

			if(migrationKetamaNodes.isEmpty()) {
				/* means fully migrated */

				allMigrationNodes.clear();
			}

			break;
		case Leave:
			/* leave all or partial hash slices from migrationKetamaNodes to ketamaNodes */
			if(migrationKetamaNodes != null && !migrationKetamaNodes.isEmpty()) {

				lock.lock();
				try {
					for (int i = keys.length - 1; i >= 0 ; i--) {
						MemcachedNode removed = migrationKetamaNodes.remove(keys[i]);

						if (removed != null) {
							ketamaNodes.remove(keys[i]);
						} else {
							// The node does not exist.
						}
					}
				} catch (IndexOutOfBoundsException e) {
					e.printStackTrace(); // optinal
				} finally {
					lock.unlock();
				}
			} else {
				// ERROR : Migration Hash Ring is null or empty
			}

			if(migrationKetamaNodes.isEmpty()) {
				allMigrationNodes.clear();
			}
			break;
		}

	}

	class KetamaIterator implements Iterator<MemcachedNode> {

		final String key;
		long hashVal;
		int remainingTries;
		int numTries = 0;

		public KetamaIterator(final String k, final int t) {
			super();
			hashVal = hashAlg.hash(k);
			remainingTries = t;
			key = k;
		}

		private void nextHash() {
			long tmpKey = hashAlg.hash((numTries++) + key);
			// This echos the implementation of Long.hashCode()
			hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
			hashVal &= 0xffffffffL; /* truncate to 32-bits */
			remainingTries--;
		}

		public boolean hasNext() {
			return remainingTries > 0;
		}

		public MemcachedNode next() {
			try {
				return getNodeForKey(hashVal);
			} finally {
				nextHash();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}

	}
}
