package com.tspowell.ttd.cache;

import com.tspowell.ttd.cache.associative.SetAssociativeCache;
import com.tspowell.ttd.cache.invalidation.CacheInvalidator;
import com.tspowell.ttd.cache.invalidation.LRUInvalidator;
import com.tspowell.ttd.cache.invalidation.MRUInvalidator;
import com.tspowell.ttd.cache.invalidation.SmallestValueInvalidator;
import org.junit.Test;

import javax.cache.Cache;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit test n-way set associative cache.
 */
public class SetAssociativeCacheTest {

    @Test
    public void testEmptyCache() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(10, 5);
        assertEquals(0, cache.size());
        assertNull(cache.get("test"));
    }

    @Test
    public void testInsertSingleObject() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(10, 5);
        cache.put("Travis", 1);

        assertEquals(Integer.valueOf(1), cache.get("Travis"));
        assertNull(cache.get("Non-Existant Key"));
    }

    @Test
    public void testInsertSingleObjectSingleSlot() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(1, 1);
        cache.put("Travis", 1);

        assertEquals(Integer.valueOf(1), cache.get("Travis"));

        assertEquals(1, cache.size());
        assertFalse(cache.isEmpty());

        cache.clear();

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertNull(cache.get("Travis"));
    }

    @Test
    public void testUpdateObject() {
        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(10, 20);
        for (int i = 0; i < 100; ++i) {
            cache.put(i, i);
        }

        for (int i = 0; i < 100; ++i) {
            cache.put(i, i * 2);
        }

        assertEquals(100, cache.size());

        for (int i = 0; i < 100; ++i) {
            assertEquals(Integer.valueOf(i * 2), cache.get(i));
        }
    }

    @Test
    public void testSimpleLRU() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(1, 2, LRUInvalidator::new);

        cache.put("Bob", 1);
        cache.put("Steve", 2);

        assertEquals(Integer.valueOf(1), cache.get("Bob"));
        assertEquals(Integer.valueOf(2), cache.get("Steve"));

        assertEquals(2, cache.size());

        cache.put("Newer Value", 3);

        assertFalse(cache.containsKey("Bob"));
        assertTrue(cache.containsKey("Steve"));
        assertTrue(cache.containsKey("Newer Value"));
        assertEquals(Integer.valueOf(3), cache.get("Newer Value"));

        assertEquals(2, cache.size());
    }

    @Test
    public void testSimpleMRU() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(1, 2, MRUInvalidator::new);

        cache.put("Bob", 1);
        cache.put("Steve", 222);

        assertEquals(Integer.valueOf(222), cache.remove("Steve"));
        cache.remove("");
        cache.put("Steve", 2);

        assertEquals(Integer.valueOf(1), cache.get("Bob"));
        assertEquals(Integer.valueOf(2), cache.get("Steve"));

        assertEquals(2, cache.size());

        cache.put("Newer Value", 3);

        assertFalse(cache.containsKey("Steve"));
        assertTrue(cache.containsKey("Bob"));
        assertTrue(cache.containsKey("Newer Value"));
        assertEquals(Integer.valueOf(3), cache.get("Newer Value"));

        assertEquals(2, cache.size());

        // Now, we should replace Newer Value as that was the most recently accessed.
        cache.put("Newest Value", 4);
        assertFalse(cache.containsKey("Newer Value"));
        assertTrue(cache.containsKey("Bob"));

        // Access Bob, make it the newest value.
        assertEquals(Integer.valueOf(1), cache.get("Bob"));
        cache.put("Last Value", 5);

        assertEquals(2, cache.size());
        assertFalse(cache.containsKey("Bob")); // was the MRU

        final Iterator<Cache.Entry<String, Integer>> itr = cache.iterator();
        final Set<String> seenKeys = cache.keySet()
                .stream()
                .collect(Collectors.toSet());

        assertTrue(cache.containsKey("Newest Value"));
        assertTrue(cache.containsKey("Last Value"));

        assertTrue(seenKeys.contains("Newest Value"));
        assertTrue(seenKeys.contains("Last Value"));

        // Add two more members, first will be overwritten by the second because it is MRU
        cache.put("One", 1);
        cache.put("Two", 2);
        assertEquals(2, cache.size());

        assertFalse(cache.containsKey("One")); // overwritten by Two
        assertTrue(cache.containsKey("Two"));
        assertTrue(cache.containsKey("Newest Value")); // Not recently touched
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBucketConfiguration_1() {
        new SetAssociativeCache<>(0, 4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBucketConfiguration_2() {
        new SetAssociativeCache<>(4, 0);
    }

    @Test
    public void testIterationThroughCache() {

        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(10, 5);
        final Map<Integer, Integer> lruSet = new HashMap<>();

        for (int i = 1; i <= 100; ++i) {
            cache.put(i, i);

            if (i > 50) {
                lruSet.put(i, i);
            }
        }

        assertTrue(lruSet.size() == cache.size());
        assertEquals(cache.size(), cache.entrySet().size());

        final Iterator<Cache.Entry<Integer, Integer>> itr = cache.iterator();

        while (itr.hasNext()) {
            final Cache.Entry<Integer, Integer> entry = itr.next();

            assertTrue(lruSet.containsKey(entry.getKey()));
            assertEquals(lruSet.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testContainsValue() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(1, 2);

        cache.put("One", 1);
        assertFalse(cache.containsValue(0));
        assertTrue(cache.containsValue(1));

        cache.put("Two", 2);
        cache.put("Three", 3);

        assertFalse(cache.containsValue(0));
        assertFalse(cache.containsValue(1));

        assertTrue(cache.containsValue(2));
        assertTrue(cache.containsValue(3));
    }

    private static class HashCollision {
        final String val;
        HashCollision(final String val_) {
            val = val_;
        }

        @Override
        public int hashCode() {
            return 11;
        }

        @Override
        public boolean equals(final Object rhs_) {
            if (!(rhs_ instanceof HashCollision)) {
                throw new RuntimeException("Not a hash collection");
            }

            final HashCollision rhs = (HashCollision)rhs_;

            if (this.val == null && rhs.val == null) {
                return true;
            }

            if (this.val != null &&
                    rhs.val != null &&
                    this.val.equals(rhs.val)) {
                return true;
            }

            return false;
        }
    }

    @Test
    public void testHashCollision() {
        final SetAssociativeCache<HashCollision, Integer> cache = new SetAssociativeCache<>(10, 10);

        final HashCollision one = new HashCollision("one");
        cache.put(one, 1);
        cache.put(new HashCollision("two"), 2);

        assertEquals(2, cache.size());
        assertTrue(cache.containsKey(new HashCollision("one")));
        assertTrue(cache.containsKey(one));

        assertEquals(Integer.valueOf(1), cache.get(one));
        assertEquals(Integer.valueOf(2), cache.get(new HashCollision("two")));

        assertEquals(Integer.valueOf(2), cache.remove(new HashCollision("two")));
        assertEquals(1, cache.size());
    }

    @Test
    public void testContainsValueObjectEquality() {
        final SetAssociativeCache<Integer, HashCollision> cache = new SetAssociativeCache<>(1, 1);

        final HashCollision one = new HashCollision("one");
        cache.put(1, one);

        assertTrue(cache.containsValue(one));
        assertTrue(cache.containsValue(new HashCollision("one")));
        assertFalse(cache.containsValue(new HashCollision("two")));
    }

    @Test
    public void testRemoveFromCache() {
        int total = 100;
        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(10, total);

        assertNull(cache.remove(-1));

        for (int i = 0; i < total; ++i) {
            cache.put(i, i);
        }

        assertEquals(total, cache.size());

        // Remove even cache entries
        for (int i = total - 1; i >= 0; --i) {
            if (i % 2 == 0) {
                cache.remove(i);
            }
        }

        assertEquals(total / 2, cache.size());

        // Only odd elements are left
        for (int i = 0; i < total; ++i) {
            if (i % 2 == 1) {
                assertEquals(Integer.valueOf(i), cache.get(i));
            }
        }

        assertEquals(total / 2, cache.entrySet().size());

        assertNull(cache.remove(total + 1));
    }

    @Test
    public void testPutAllAndValues() {
        final Map<Integer, Integer> m = new HashMap<>();
        m.put(1,2);
        m.put(3,4);

        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(10, 10);
        cache.putAll(m);

        assertEquals(2, cache.size());
        assertEquals(Integer.valueOf(2), cache.get(1));
        assertEquals(Integer.valueOf(4), cache.get(3));

        final Collection<Integer> values = cache.values();
        assertEquals(cache.size(), values.size());

        for (Integer i : values) {
            assertTrue(cache.containsValue(i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnwrapBadRow() {
        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(1, 1, () ->
                new CacheInvalidator<Integer, Integer>() {
                    @Override
                    public void touch(UnsettableEntry entry) {
                        entry.unwrap(HashMap.class);
                    }

                    @Override
                    public void remove(UnsettableEntry entry) {
                    }

                    @Override
                    public boolean invalidate() {
                        return false;
                    }
                });
        cache.put(1, 1);
        cache.put(2, 2);
        assertEquals(Integer.valueOf(2), cache.get(2));
    }

    @Test(expected = RuntimeException.class)
    public void testBadCacheInvalidationStrategy() {
        final SetAssociativeCache<Integer, Integer> cache = new SetAssociativeCache<>(1, 1, () ->
            new CacheInvalidator<Integer, Integer>() {
                @Override
                public void touch(UnsettableEntry<Integer, Integer> entry) {
                }

                @Override
                public void remove(UnsettableEntry<Integer, Integer> entry) {
                }

                @Override
                public boolean invalidate() {
                    return false;
                }
            });

        cache.put(1, 1);
        cache.put(2, 2);
        assertEquals(Integer.valueOf(2), cache.get(2));
    }

    @Test
    public void testDefaultInvalidationEmptyBucket() {
        final CacheInvalidator
                lru = new LRUInvalidator(),
                mru = new MRUInvalidator(),
                smallest = new SmallestValueInvalidator();

        assertFalse(lru.invalidate());
        assertFalse(mru.invalidate());
        assertFalse(smallest.invalidate());
    }


    @Test
    public void testSmallestInvalidator() {
        final SetAssociativeCache<String, Integer> cache = new SetAssociativeCache<>(1, 3, SmallestValueInvalidator::new);
        cache.put("two", 2);
        cache.put("one", 1);
        cache.put("three", 3);
        cache.put("four", 4);

        assertEquals(3, cache.size());
        assertEquals(new HashSet<>(Arrays.asList("two", "three", "four")), cache.keySet());
    }
}
