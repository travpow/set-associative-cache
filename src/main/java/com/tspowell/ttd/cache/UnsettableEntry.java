package com.tspowell.ttd.cache;

import javax.cache.Cache;

public interface UnsettableEntry<K, V> extends Cache.Entry<K, V> {
    void unset();
}
