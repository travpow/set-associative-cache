package com.tspowell.ttd.cache.invalidation;

import com.tspowell.ttd.cache.UnsettableEntry;

public interface CacheInvalidator<K, V> {
    void touch(UnsettableEntry<K, V> entry);
    void remove(UnsettableEntry<K, V> entry);
    /**
     * @return true if an item was removed, false if no items were removed.
     */
    boolean invalidate();
}