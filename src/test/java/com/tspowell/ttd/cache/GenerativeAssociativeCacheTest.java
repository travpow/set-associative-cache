package com.tspowell.ttd.cache;

import com.tspowell.ttd.cache.associative.SetAssociativeCache;
import org.junit.Test;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GenerativeAssociativeCacheTest {

    @Test
    public void generativeTest() {
        int permutations = 32;

        for (int numberOfSets = 1; numberOfSets < permutations; ++numberOfSets) {
            for (int entriesPerSet = 1; entriesPerSet < permutations; ++entriesPerSet) {
                for (int i = 1; i <= 10; ++i) {
                    testIteratorAndRetrieveInOtherOrder(numberOfSets, entriesPerSet, i);
                }
            }
        }
    }

    private void testIteratorAndRetrieveInOtherOrder(int numberOfSets, int entriesPerSet, int multiplier) {
        final int totalEntries = numberOfSets * entriesPerSet;

        final SetAssociativeCache<Integer, String> cache = new SetAssociativeCache<>(numberOfSets, entriesPerSet);

        for (int i = 1; i <= totalEntries * multiplier; ++i) {
            cache.put(i, String.valueOf(i));
        }

        // Most of the items will have been purged by the LRU, so we're going to take inventory of
        // the remaining items in the cache.
        final Map<Integer, String> workingCache = new HashMap<>();

        for (Cache.Entry<Integer, String> entry : cache) {
            workingCache.put(entry.getKey(), entry.getValue());
        }

        assertEquals(totalEntries, workingCache.size());
        assertEquals(workingCache.size(), cache.size());

        for (Map.Entry<Integer, String> elem : workingCache.entrySet()) {
            assertEquals(elem.getValue(), cache.get(elem.getKey()));
        }
    }
}
