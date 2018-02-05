/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import java.util.Objects;

/**
 * Interface of entries in a state table. Entries are triple of key, namespace, and state.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
public abstract class StateEntry<K, N, S> {
	/**
	 * The key. Assumed to be immutable and not null.
	 */
	final K key;

	/**
	 * The namespace. Assumed to be immutable and not null.
	 */
	final N namespace;

	/**
	 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
	 */
	S state;

	/**
	 * Link to another {@link StateEntry}. This is used to resolve collisions in the
	 * {@link CopyOnWriteStateTable} through chaining.
	 */
	StateEntry<K, N, S> next;

	/**
	 * The version of this {@link StateEntry}. This is meta data for copy-on-write of the table structure.
	 */
	int entryVersion;

	/**
	 * The version of the state object in this entry. This is meta data for copy-on-write of the state object itself.
	 */
	int stateVersion;

	/**
	 * The computed secondary hash for the composite of key and namespace.
	 */
	final int hash;

	StateEntry() {
		this(null, null, null, 0, null, 0, 0);
	}

	StateEntry(StateEntry<K, N, S> other, int entryVersion) {
		this(other.key, other.namespace, other.state, other.hash, other.next, entryVersion, other.stateVersion);
	}

	StateEntry(
			K key,
			N namespace,
			S state,
			int hash,
			StateEntry<K, N, S> next,
			int entryVersion,
			int stateVersion) {
		this.key = key;
		this.namespace = namespace;
		this.hash = hash;
		this.next = next;
		this.entryVersion = entryVersion;
		this.state = state;
		this.stateVersion = stateVersion;
	}

	/**
	 * Returns the key of this entry.
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Returns the namespace of this entry.
	 */
	public N getNamespace() {
		return namespace;
	}

	/**
	 * Returns the state of this entry.
	 */
	public S getState() {
		return state;
	}

	/**
	 * Returns the expiration time of state
	 */
	public int getExpirationTime() {
		return 0;
	}

	@Override
	public final boolean equals(Object o) {
		if (!(o instanceof StateEntry)) {
			return false;
		}

		StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
		return e.getKey().equals(key)
				&& e.getNamespace().equals(namespace)
				&& Objects.equals(e.getState(), state);
	}

	@Override
	public final int hashCode() {
		return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
	}

	@Override
	public final String toString() {
		return "(" + key + "|" + namespace + ")=" + state;
	}
}
