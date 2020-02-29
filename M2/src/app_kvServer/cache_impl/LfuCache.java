package cache_impl;

import java.util.*;
import java.util.concurrent.Semaphore;

import app_kvServer.CacheManager;
import app_kvServer.IKVServer.CacheStrategy;

public class LfuCache extends CacheManager {

    public LfuCache(int cacheSize, CacheStrategy cacheStrategy) {
        super(cacheSize, cacheStrategy);
        mapValues = new HashMap<>();
        mapCounts = new HashMap<>();
        mapOrders = new HashMap<>();
        mapOrders.put(1, new LinkedHashSet<String>());
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        lock.acquire();
        if (mapValues.containsKey(key)) {
            mapValues.put(key, value);
            lock.release();
            return;
        }

        try {
            // Evict.
            if (mapValues.size() >= cacheSize) {
                String evictKey = mapOrders.get(MinFrequency).iterator().next();
                mapOrders.get(MinFrequency).remove(evictKey);
                mapValues.remove(evictKey);
                mapCounts.remove(evictKey);
            }
            mapValues.put(key, value);
            mapCounts.put(key, 1);
            MinFrequency = 1;
            mapOrders.get(1).add(key);
        } catch (Exception e) {
            return;
        } finally {
            lock.release();
        }

    }

    @Override
    public String getKV(String key) throws Exception {
        lock.acquire();
        if (!mapValues.containsKey(key)) {
            lock.release();
            return null;
        }

        try {
            int count = mapCounts.get(key);
            mapCounts.put(key, count + 1);
            mapOrders.get(count).remove(key);

            if (count == MinFrequency && mapOrders.get(count).size() == 0)
                MinFrequency++;
            if (!mapOrders.containsKey(count + 1))
                mapOrders.put(count + 1, new LinkedHashSet<String>());
            mapOrders.get(count + 1).add(key);

            String result = mapValues.get(key);
            return result;
        } catch (Exception e) {
            return null;
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean inCache(String key) throws Exception {
        lock.acquire();
        boolean result = mapValues.containsKey(key);
        lock.release();
        return result;
    }

    @Override
    public void delete(String key) throws Exception {
        lock.acquire();
        if (!mapValues.containsKey(key)) {
            lock.release();
            return;
        }

        try {
            mapValues.remove(key);
            mapOrders.get(mapCounts.get(key)).remove(key);
            mapCounts.remove(key);

            // Check if there is at least one element in the cache
            if (!mapValues.isEmpty()) {
                MinFrequency = -1;
            } else {
                int min = Integer.MAX_VALUE;
                for (int i : mapOrders.keySet()) {
                    if (mapOrders.get(i).isEmpty()) continue;
                    if (i < min) min = i;
                }
                MinFrequency = min;
            }
        } catch (Exception e){
            return;
        } finally {
            lock.release();
        }
    }

    @Override
    public void clearCache() throws Exception {
        lock.acquire();
        mapValues.clear();
        mapOrders.clear();
        mapOrders.put(1, new LinkedHashSet<String>());
        mapCounts.clear();
        MinFrequency = -1;
        lock.release();
    }

    private int MinFrequency = -1;
    public Map<String, String> mapValues;
    public Map<String, Integer> mapCounts;
    public Map<Integer, LinkedHashSet<String>> mapOrders;
    private final Semaphore lock = new Semaphore(1);

}

