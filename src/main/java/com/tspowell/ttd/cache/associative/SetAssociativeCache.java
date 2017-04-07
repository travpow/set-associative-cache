package com.tspowell.ttd.cache.associative;

import com.tspowell.ttd.cache.UnsettableEntry;
import com.tspowell.ttd.cache.invalidation.CacheInvalidator;
import com.tspowell.ttd.cache.invalidation.InvalidationException;
import com.tspowell.ttd.cache.invalidation.LRUInvalidator;

import javax.cache.Cache;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Supplier;

/**
 * An N-way set-associative cache.
 *
 * A fixed-size bucket array points to bucket sets with a member cache invalidator.
 * - For an LRU invalidator, the head designates the least recently used object. The tail is the most recently used.
 * - Only get(), put() and remove() affect the cache invalidation list.
 *
 * Because we can't take advantage of contiguously allocated blocks of cache entries, we are pre-allocating
 * the cache entries and updating the pointers to the key/value/hash when setting a value. The entries are
 * maintained for the lifetime of the cache, and iterators must allocate new entries.
 *
 * TODO:
 * Normally, I would implement the JCache API (and I have included it as a dependency
 * of this project specifically to use the javax.cache.Cache.Entry interface), but
 * there's a large number of helper methods that I would prefer to implement in a second pass.
 *
 * @param <K> key class
 * @param <V> value class
 */
public class SetAssociativeCache<K, V>
        implements Map<K, V>, Iterable<Cache.Entry<K, V>> {
    private final Supplier<CacheInvalidator<K, V>> invalidatorSupplier;
    private final int numberOfSets;
    private final int entriesPerSet;
    private int size = 0;

    private Bucket[] buckets;

    /**
     * ctor
     * @param numberOfSets number of sets
     * @param entriesPerSet entries per set (the N in N-Way)
     */
    public SetAssociativeCache(
            final int numberOfSets,
            final int entriesPerSet) {
        this.invalidatorSupplier = LRUInvalidator::new;
        this.numberOfSets = numberOfSets;
        this.entriesPerSet = entriesPerSet;

        initialize();
    }

    /**
     * ctor with a cache invalidation strategy
     * @param numberOfSets number of sets
     * @param entriesPerSet entries per set (the N in N-Way)
     */
    public SetAssociativeCache(
            final int numberOfSets,
            final int entriesPerSet,
            final Supplier<CacheInvalidator<K, V>> invalidator) {
        this.invalidatorSupplier = invalidator;
        this.numberOfSets = numberOfSets;
        this.entriesPerSet = entriesPerSet;

        initialize();
    }

    @SuppressWarnings("unchecked")
    private void initialize() {
        if (this.numberOfSets < 1 || this.entriesPerSet < 1) {
            throw new IllegalArgumentException("Must configure at least one set, and one entry per set.");
        }

        this.buckets = (Bucket[]) Array.newInstance(Bucket.class, this.numberOfSets);

        for (int i = 0; i < this.numberOfSets; ++i) {
            this.buckets[i] = new Bucket();
        }
    }

    @Override
    public Iterator<Cache.Entry<K, V>> iterator() {
        return new SetAssociativeIterator();
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        int hash = key.hashCode();

        for (final Entry entry : bucketForHash(hash).entries) {
            if (isMatch(entry, hash, key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        for (final Bucket bucket : this.buckets) {
            for (final Entry entry : bucket.entries) {
                if (entry.isSet() &&
                        (entry.getValue() == value ||
                                entry.getValue().equals(value))) {
                    return true;
                }
            }
        }

        return false;
    }

    private Bucket bucketForHash(int hashCode) {
        return this.buckets[Math.abs(hashCode) % this.numberOfSets];
    }

    private int indexForBucket(int hashCode) {
        return Math.abs(hashCode) % this.entriesPerSet;
    }

    /**
     * Compare a set entry with a key for equality.
     * @param entry the proposed entry
     * @param hash the target hash
     * @param key the target key
     * @return true if the key is equal to the entry key.
     */
    private boolean isMatch(final Entry entry, final int hash, final Object key) {
        return entry.isSet() &&
                (entry.getKey() == key ||
                (entry.hash() == hash && entry.getKey().equals(key)));
    }

    /**
     * Return the entry from the set for a given key
     * @param key given
     * @param hash pre-computed hash of the key
     * @return Entry found or null
     */
    private Entry getEntry(Object key, int hash) {
        final Bucket bucket = bucketForHash(hash);
        final int startIndex = indexForBucket(hash);
        int index = startIndex;

        do {
            final Entry entry = bucket.entries[index];
            if (isMatch(entry, hash, key)) {
                bucket.touch(entry);
                return entry;
            }

            if (++index == this.entriesPerSet) {
                index = 0;
            }
        } while (index != startIndex);

        return null;
    }

    @Override
    public V get(Object key) {
        final int hash = key.hashCode();
        final Entry entry = getEntry(key, hash);

        if (entry != null) {
            return entry.getValue();
        }

        return null;
    }

    @Override
    public V put(K key, V value) {
        int hash = key.hashCode();
        final Bucket bucket = bucketForHash(hash);

        final int startIndex = indexForBucket(hash);
        int index = startIndex;
        SetAssociativeCache.Entry lastUnset = null;

        if (bucket.size == this.entriesPerSet) {
            invalidateAndCount(bucket);
        }

        do {
            final Entry entry = bucket.entries[index];
            if (isMatch(entry, hash, key)) {
                bucket.touch(entry);
                final V oldValue = entry.getValue();
                entry.setValue(value);
                bucket.touch(entry);

                return oldValue;

            } else if (!entry.isSet()) {
                lastUnset = entry;
            }

            if (++index == this.entriesPerSet) {
                index = 0;
            }
        } while (index != startIndex);

        lastUnset.setKey(key);
        lastUnset.setValue(value);
        lastUnset.setHash(hash);

        bucket.touch(lastUnset);

        bucket.size++;
        this.size++;

        return value;
    }

    /**
     * Invalidate the LRU item in this bucket and resize
     *
     * @param bucket the bucket to remove one or more cache items from, according to the algorithm for this
     *               particular cache instance.
     */
    private void invalidateAndCount(final Bucket bucket) {
        boolean removed = bucket.invalidate();

        if (removed) {
            this.size--;
        } else {
            throw new InvalidationException("Could not invalidate the bucket");
        }
    }

    /**
     * Remove a cache entry by key
     *
     * @param key to remove from the cache
     * @return the previous value stored at that key
     */
    @Override
    public V remove(Object key) {
        final int hash = key.hashCode();
        final Bucket bucket = bucketForHash(hash);

        for (final Entry entry : bucket.entries) {
            if (isMatch(entry, hash, key)) {
                this.size--;
                final V prevValue = entry.getValue();
                bucket.remove(entry);

                return prevValue;
            }
        }

        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (final Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Mark all entries in all buckets as unset.
     * Remove the iterable pointers from the buckets.
     */
    @Override
    public void clear() {
        for (final Bucket bucket : this.buckets) {
            for (final Entry entry : bucket.entries) {
                entry.unset();
            }
        }

        this.size = 0;
    }

    /**
     * Create a set of all keys in the cache.
     * @return a hash set containing the keys.
     */
    @Override
    public Set<K> keySet() {
        final Set<K> set = new HashSet<>();

        for (final Cache.Entry<K, V> entry : this) {
            set.add(entry.getKey());
        }

        return set;
    }

    /**
     * Create a list of all values in the cache.
     * @return an arraylist containing the values.
     */
    @Override
    public Collection<V> values() {
        final List<V> l = new ArrayList<>();

        for (final Cache.Entry<K, V> entry : this) {
            l.add(entry.getValue());
        }

        return l;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Map.Entry<K, V>> entrySet() {
        final Set<Map.Entry<K, V>> set = new HashSet<>();

        for (Cache.Entry<K, V> entry : this) {
            set.add(new Entry((SetAssociativeCache.Entry)entry));
        }

        return set;
    }

    public class SetAssociativeIterator implements Iterator<Cache.Entry<K, V>> {
        private int bucketIdx;
        private int entryIdx;

        public SetAssociativeIterator() {
            this.bucketIdx = 0;
            this.entryIdx = 0;
        }

        @Override
        public boolean hasNext() {
            getNextBucket();

            // don't need to check the entries Idx because that is checked by getNextBucket()
            return this.bucketIdx < numberOfSets;
        }

        @Override
        public Cache.Entry<K, V> next() {
            getNextBucket();

            // return a copy of this entry, as the entry in the bucket will be updated in-place
            final Entry entry = new Entry(current());
            this.entryIdx++;

            return entry;
        }

        private Entry current() {
            return buckets[this.bucketIdx].entries[this.entryIdx];
        }

        private void getNextBucket() {
            while (bucketIdx < buckets.length &&
                    (entryIdx >= entriesPerSet || !current().isSet())) {

                if (this.entryIdx < entriesPerSet) {
                    entryIdx++;
                } else {
                    bucketIdx++;
                    entryIdx = 0;
                }
            }
        }
    }

    private class Bucket {
        private final Entry[] entries;
        private final CacheInvalidator<K, V> invalidator;
        private int size = 0;

        @SuppressWarnings("unchecked")
        protected Bucket() {
            this.entries = (Entry[]) Array.newInstance(Entry.class, entriesPerSet);

            for (int i = 0; i < entriesPerSet; ++i) {
                this.entries[i] = new Entry();
            }

            this.invalidator = invalidatorSupplier.get();
        }

        boolean invalidate() {
            boolean removed = invalidator.invalidate();
            if (removed) {
                this.size--;
            }

            return removed;
        }

        void touch(final Entry entry) {
            this.invalidator.touch(entry);
        }

        void remove(final Entry entry) {
            this.invalidator.remove(entry);
            entry.unset();
            this.size--;
        }

        int size() {
            return this.size;
        }
    }

    /**
     * An associative set entry with previous and next pointers to the older or more recent additions to the set.
     * Implements a map entry so the cache can be queried like a map.
     */
    public class Entry
            implements Map.Entry<K, V>, UnsettableEntry<K, V> {
        private boolean isSet;
        private K key;
        private V value;
        private int hash;

        protected Entry() {
            this.isSet = false;
        }

        protected Entry(final Entry other) {
            this.key = other.getKey();
            this.value = other.getValue();
            this.hash = other.hash();
            this.isSet = other.isSet();
        }


        public int hash() {
            return this.hash;
        }

        /**
         * Remove references to keys/values on the heap, remove pointers to adjacent nodes, and flip the set bit.
         */
        public void unset() {
            this.key = null;
            this.value = null;
            this.isSet = false;
        }

        public boolean isSet() {
            return isSet;
        }

        @Override
        public K getKey() {
            return this.key;
        }

        @Override
        public V getValue() {
            return this.value;
        }

        protected void setKey(K key) {
            this.key = key;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            this.isSet = true;

            return this.value;
        }

        public void setHash(int hash) {
            this.hash = hash;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T unwrap(Class<T> clazz) {
            if (!clazz.equals(Entry.class)) {
                throw new IllegalArgumentException("Not an internal associative cache entry class!");
            }

            return (T)this;
        }
    }
}