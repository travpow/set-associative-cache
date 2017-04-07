package com.tspowell.ttd.cache.invalidation;

import com.tspowell.ttd.cache.invalidation.lru.LinkedLRUList;
import com.tspowell.ttd.cache.invalidation.lru.LinkedEntry;
import com.tspowell.ttd.cache.UnsettableEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * O(1) cache invalidation for a linked entry list in a bucket, using a Most Recently Used algorithm
 */
public class MRUInvalidator<K, V>
        extends LinkedLRUList<K, V> implements CacheInvalidator<K, V> {

    // This map is an unfortunate necessity. We don't want to have to traverse through the whole list to find
    // our matching object.
    final Map<K, LinkedEntry<K, V>> m = new HashMap<>();

    /**
     * Append to the end of the use-ordered linked list for this bucket.
     * Least recently used items are closer to the head, more recently used at the tail.
     *
     * @param entry to move to the end of list
     */
    @Override
    public void touch(final UnsettableEntry<K, V> entry) {

        final K key = entry.getKey();
        LinkedEntry<K, V> linked;

        if (m.containsKey(key)) {
            linked = m.get(key);
        } else {
            linked = new LinkedEntry<>(entry);
            m.put(key, linked);
        }

        markRecentlyUsed(linked);
    }

    /**
     * Remove this item from the bucket invalidation index
     *
     * @param entry to remove
     */
    @Override
    public void remove(final UnsettableEntry<K, V> entry) {
        final LinkedEntry<K, V> inMap = m.get(entry.getKey());

        if (inMap != null) {
            removeEntry(inMap);
            m.remove(entry.getKey());
        }
    }

    @Override
    public boolean invalidate() {
        final LinkedEntry<K, V> toRemove = tail;

        if (toRemove == null) {
            return false;
        }

        removeEntry(toRemove);
        toRemove.unset();

        return true;
    }
}
