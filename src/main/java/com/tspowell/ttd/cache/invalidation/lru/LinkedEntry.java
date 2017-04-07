package com.tspowell.ttd.cache.invalidation.lru;

import com.tspowell.ttd.cache.UnsettableEntry;

public class LinkedEntry<K, V> {

    UnsettableEntry<K, V> entry;
    LinkedEntry<K, V> next, prev;

    public LinkedEntry(final UnsettableEntry<K, V> entry) {
        this.entry = entry;
        this.next = null;
        this.prev = null;
    }

    public LinkedEntry<K, V> next() {
        return this.next;
    }

    public LinkedEntry<K, V> previous() {
        return this.prev;
    }

    public void setNext(final LinkedEntry<K, V> next) {
        this.next = next;
    }

    public void setPrevious(final LinkedEntry<K, V> previous) {
        this.prev = previous;
    }

    public void unset() {
        entry.unset();
    }
}
