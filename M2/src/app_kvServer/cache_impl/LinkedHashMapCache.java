package cache_impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class LinkedHashMapCache extends LinkedHashMap<String, String> {
    private int cacheSize;

    public LinkedHashMapCache(int cacheSize, boolean isAccessOrder) {
        super(16, 0.75f, isAccessOrder);
        this.cacheSize = cacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > cacheSize;
    }
}
