package com.tspowell.ttd.cache.invalidation;

import com.tspowell.ttd.cache.UnsettableEntry;

import java.util.PriorityQueue;

/**
 * O(1) cache invalidation for the smallest element in a bucket
 */
public class SmallestValueInvalidator<K, V extends Comparable<V>>
        implements CacheInvalidator<K, V> {

    final PriorityQueue<UnsettableEntry<K, V>> q = new PriorityQueue<>((left, right) -> {
        return left.getValue().compareTo(right.getValue());
    });

    /**
     * Append to the end of the use-ordered linked list for this bucket.
     * Least recently used items are closer to the head, more recently used at the tail.
     *
     * @param entry to move to the end of list
     */
    @Override
    public void touch(final UnsettableEntry<K, V> entry) {
        if (!q.contains(entry)) {
            q.add(entry);
        }
    }

    /**
     * Remove this item from the bucket invalidation index
     *
     * @param entry to remove
     */
    @Override
    public void remove(final UnsettableEntry<K, V> entry) {
        q.remove(entry);
    }

    @Override
    public boolean invalidate() {
        if (q.size() == 0) {
            return false;
        }

        final UnsettableEntry<K, V> toRemove = q.poll();
        toRemove.unset();

        return true;
    }
}
