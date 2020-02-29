package cache_impl;

import app_kvServer.CacheManager;
import app_kvServer.IKVServer.CacheStrategy;

import java.util.concurrent.Semaphore;

public class FifoCache extends CacheManager {

    public FifoCache(int cacheSize, CacheStrategy cacheStrategy) {
        super(cacheSize, cacheStrategy);
        map = new LinkedHashMapCache(cacheSize, /*isAccessOrder=*/ false);
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // LinkedHashMapCache takes care of LRU logic.
        lock.acquire();
        map.put(key, value);
        lock.release();
    }

    @Override
    public String getKV(String key) throws Exception {
        lock.acquire();
        if (!map.containsKey(key)) {
            lock.release();
            return null;
        }

        String result = map.get(key);
        lock.release();
        return result;
    }

    @Override
    public boolean inCache(String key) throws Exception {
        lock.acquire();
        boolean result = map.containsKey(key);
        lock.release();
        return result;
    }

    @Override
    public void delete(String key) throws Exception {
        lock.acquire();
        if (!map.containsKey(key)) {
            lock.release();
            return;
        }

        map.remove(key);
        lock.release();
    }

    @Override
    public void clearCache() throws Exception {
        lock.acquire();
        map.clear();
        lock.release();
    }

    public LinkedHashMapCache map;
    private final Semaphore lock = new Semaphore(1);

}
