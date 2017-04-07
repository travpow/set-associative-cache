package com.tspowell.ttd.cache.invalidation.lru;

public abstract class LinkedLRUList<K, V> {

    protected LinkedEntry<K, V> head;
    protected LinkedEntry<K, V> tail;

    public LinkedEntry<K, V> head() {
        return head;
    }

    public LinkedEntry<K, V> tail() {
        return tail;
    }

    public void setHead(final LinkedEntry<K, V> head) {
        this.head = head;
    }

    public void setTail(final LinkedEntry<K, V> tail) {
        this.tail = tail;
    }

    /**
     * Remove the entry from the bucket's linked list
     * @param entry The entry to remove
     */
     protected void removeEntry(LinkedEntry<K, V> entry) {
        final LinkedEntry<K, V> prev = entry.previous();
        final LinkedEntry<K, V> next = entry.next();

        if (head() == entry) {
            setHead(next);
        }

        if (tail() == entry) {
            setTail(prev);
        }

        if (prev != null) {
            prev.setNext(next);
        }

        if (next != null) {
            next.setPrevious(prev);
        }
    }

    protected void markRecentlyUsed(final LinkedEntry<K, V> entry) {
        removeEntry(entry);

        // Existing tail
        if (tail() != null) {
            tail().setNext(entry);
        }

        entry.setPrevious(tail());
        entry.setNext(null);
        setTail(entry);

        // First item in list
        if (head() == null) {
            setHead(entry);
        }
    }
}
