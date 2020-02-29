package app_kvServer;

import cache_impl.FifoCache;
import cache_impl.LfuCache;
import cache_impl.LruCache;

public class CacheFactory {

    public static CacheManager GetCache(int cacheSize, IKVServer.CacheStrategy cacheStrategy) {
        assert(cacheSize > 0);
        assert(cacheStrategy != IKVServer.CacheStrategy.None);

        switch (cacheStrategy) {
            case FIFO:
                return new FifoCache(cacheSize, cacheStrategy);
            case LRU:
                return new LruCache(cacheSize, cacheStrategy);
            case LFU:
                return new LfuCache(cacheSize, cacheStrategy);
            default:
                return null;
        }
    }

}
