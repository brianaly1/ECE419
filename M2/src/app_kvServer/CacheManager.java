package app_kvServer;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.HashMap;
import java.util.Map;

public abstract class CacheManager {

    protected int cacheSize;
    private CacheStrategy cacheStrategy;

    public CacheManager(int cacheSize, CacheStrategy cacheStrategy) {
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
    }

    public int getCacheSize(){
        return this.cacheSize;
    }

    public CacheStrategy getCacheStrategy(){
        return this.cacheStrategy;
    }

    // Override these methods.
    public abstract void putKV(String key, String value) throws Exception;
    public abstract String getKV(String key) throws Exception;
    public abstract boolean inCache(String key) throws Exception;
    public abstract void delete(String key) throws Exception;
    public abstract void clearCache() throws Exception;

}
