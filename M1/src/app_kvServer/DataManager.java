package app_kvServer;

import java.util.HashMap; 
import org.apache.log4j.Logger;

import app_kvServer.CacheFactory;
import app_kvServer.FileManager;
import app_kvServer.IKVServer.CacheStrategy;

public class DataManager {

	private static Logger logger = Logger.getRootLogger();
    private int cacheSize;
    private CacheStrategy strategy;
    private FileManager fileManager;
    private CacheManager cacheManager;

	/**
	 * Instantiate a DataManager object
	 * @param logger KVServer will pass its logger 
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public DataManager(int cacheSize, CacheStrategy strategy) {
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.fileManager = new FileManager();
		this.cacheManager = CacheFactory.GetCache(cacheSize, strategy);
	}

	public boolean inStorage(String key) throws Exception {
		return fileManager.inStorage(key);
	}

	public boolean inCache(String key) throws Exception {
		return cacheManager.inCache(key);
	}

	public String getKV(String key) throws Exception{
		// First try: find in Cache.
		String value = cacheManager.getKV(key);
		if (value != null) {
			System.out.println("DataManager:getKV: CacheManager returned not NULL");
			return value;
		}
		System.out.println("DataManager:getKV: CacheManager returned NULL");

		// Second try: find in storage, and update cache.
		value = fileManager.getKV(key);
		if (value!=null)
			cacheManager.putKV(key, value);
		return value;
	}

	public void putKV(String key, String value) throws Exception {
		fileManager.putKV(key, value);
			cacheManager.putKV(key, value);
	}

	public void delete(String key) throws Exception {
		fileManager.delete(key);
		cacheManager.delete(key);
	}

	public void update(String key, String value) throws Exception {
		delete(key);
		putKV(key, value);
	}

	public void clearCache() throws Exception {
		cacheManager.clearCache();
	}

	public void clearStorage() throws Exception {
		fileManager.clearStorage();
		clearCache();
	}
}
