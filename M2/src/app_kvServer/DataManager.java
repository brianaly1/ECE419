package app_kvServer;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import ecs.ECSHashRing;
import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;

public class DataManager {

    private static Logger logger = Logger.getRootLogger();

    private String nodeName;
    private FileManager fileManager;
    private CacheManager cacheManager;
    private int cacheSize;
    private CacheStrategy strategy;

    // HashRing.
    public ECSHashRing hashRing;
    public String currentFullAddress;
    private String currentNodeHash;

    // Connection to other servers.
    private ServerConnection serverConnection;

    // Will be acquired when callback thread performs purge.
    // KVServer needs this lock before performing any data ops.
    // KVServer will throw an exception to ClientConnection if lock
    // is acquired.
    private Semaphore purgeLock = new Semaphore(1);

    /**
     * Instantiate a DataManager object
     *
     * @param logger    KVServer will pass its logger
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public DataManager(int cacheSize, CacheStrategy strategy, String nodeName, String serverAdress) {
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.nodeName = nodeName;
        this.fileManager = new FileManager(nodeName);
        this.cacheManager = CacheFactory.GetCache(cacheSize, strategy);
        this.currentFullAddress = serverAdress;
        this.hashRing = new ECSHashRing();
    }

    public DataManager(int cacheSize, CacheStrategy strategy, String serverAdress) {
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.nodeName = null;
        this.fileManager = new FileManager();
        this.cacheManager = CacheFactory.GetCache(cacheSize, strategy);
        this.currentFullAddress = serverAdress;
    }

    public boolean inStorage(String key) throws Exception {
        return fileManager.inStorage(key);
    }

    public boolean inCache(String key) throws Exception {
        return cacheManager.inCache(key);
    }

    // Checks if the key is within this running instance ipAddress:port hash range.
    public boolean inHashRange(String key) throws Exception {
        System.out.print("GET NODE BY KEY CALLED ............................................................................ \n \n");
        if (isUnityRing()){
            return true;
        }
        String serverAddr = hashRing.getNodeByKey(key);
        return currentFullAddress.equals(serverAddr);
    }

    public String getKV(String key) throws Exception {
        // First try: find in Cache.
        String value = cacheManager.getKV(key);
        if (value != null) {
            System.out.println("DataManager:getKV: CacheManager returned not NULL");
            return value;
        }
        System.out.println("DataManager:getKV: CacheManager returned NULL");

        // Second try: find in storage, and update cache.
        value = fileManager.getKV(key);
        if (value != null)
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

    public void initialize(String rootDataString, String fullAddress){
        this.hashRing = new ECSHashRing(rootDataString);
        this.currentFullAddress = fullAddress;
        this.currentNodeHash = ECSHashRing.getHash(currentFullAddress);
    }

	public void purge(boolean purgeAll) throws Exception{
        purgeLock.acquire();
        // Find the fullAddress of other servers and all the keys that now belongs to the servers in hash range.
		Map<String,Vector<String>> addressToKeysMap = new HashMap<String,Vector<String>>();
		Vector<String> keys = fileManager.getKeys();
		for (String key : keys) {
            if (!purgeAll) 
		        if (inHashRange(key)) continue;
		    String fullAddress = hashRing.getNodeByKey(key);

		    // Check if it's added, if not add an empty vector
		    if (!addressToKeysMap.containsKey(fullAddress))
		        addressToKeysMap.put(fullAddress, new Vector<String>());
            addressToKeysMap.get(fullAddress).add(key);
		}

		// Make a connection to the corresponding server and hand off data.
        for (Map.Entry<String, Vector<String>> addressToKeys : addressToKeysMap.entrySet()) {
            try {
                serverConnection = new ServerConnection(addressToKeys.getKey());
            } catch (Exception e) {
                logger.error("Server was not successful on making a connection to another server for data off-loading");
                continue;
            }
            for (String kvKey : addressToKeys.getValue()) {
                // Get value from the file manager and call put.
                try {
                    String value = fileManager.getKV(kvKey);
                    serverConnection.put(kvKey, value);
                    delete(kvKey);
                } catch (Exception e) {
                    logger.error("Server found error finding and sending key and value to another server.");
                }
            }
        }
        purgeLock.release();
	}

    public boolean isActive(){
        System.out.print("\n \n checkActive ................................................. \n \n");
        System.out.print(hashRing.inRing(currentFullAddress));
        return hashRing.inRing(currentFullAddress);
    }

    public boolean isUnityRing(){
        return hashRing.isUnity();
    }

    public boolean getLock() throws Exception {
        return purgeLock.tryAcquire();
    }

    public void releaseLock() throws Exception {
        purgeLock.release();
    }

}
