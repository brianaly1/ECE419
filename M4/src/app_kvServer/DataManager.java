package app_kvServer;

import java.util.*;
import java.util.concurrent.Semaphore;
import server_exceptions.ServerToServerErrorException;

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
    // Checks if the key belongs to any of the coordinators using this server as a replica.
    public boolean inNewReplicationRange(String key) throws Exception {
        HashSet<String> coordinators = hashRing.getCoordinators(currentFullAddress);
        if (coordinators == null) return false;
        String serverAddr = hashRing.getNodeByKey(key);
        return coordinators.contains(serverAddr);
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
        Map<String,Vector<String>> addressToKeysMapReplicas = new HashMap<String,Vector<String>>();
		Vector<String> keys = fileManager.getKeys();
		for (String key : keys) {
            String fullAddress = hashRing.getNodeByKey(key);
            if (!purgeAll) 
		        if (inHashRange(key)) continue;
                if (inNewReplicationRange(key)){
                    if (!addressToKeysMapReplicas.containsKey(fullAddress))
                        addressToKeysMapReplicas.put(fullAddress, new Vector<String>());
                    addressToKeysMapReplicas.get(fullAddress).add(key);
                    continue;
                }

		    // Check if it's added, if not add an empty vector
		    if (!addressToKeysMap.containsKey(fullAddress))
		        addressToKeysMap.put(fullAddress, new Vector<String>());
            addressToKeysMap.get(fullAddress).add(key);
		}

		// Make a connection to the corresponding server and hand off data.
        serverToServer(addressToKeysMap, /*toReplicate=*/false, /*deleteKeys=*/true);
        if (!addressToKeysMapReplicas.isEmpty())
            serverToServer(addressToKeysMapReplicas, /*toReplicate=*/false, /*deleteKeys=*/false);
        purgeLock.release();
    }

    public void serverToServer(Map<String,Vector<String>> addressToKeysMap, boolean toReplicate, boolean deleteKeys) throws Exception{
        for (Map.Entry<String, Vector<String>> addressToKeys : addressToKeysMap.entrySet()) {
            try {
                serverConnection = new ServerConnection(addressToKeys.getKey());
            } catch (Exception e) {
                logger.error("Server was not successful on making a connection to another server for data off-loading", e);
                continue;
            }
            boolean success;
            for (String kvKey : addressToKeys.getValue()) {
                // Get value from the file manager and call put.
                try {
                    String value = fileManager.getKV(kvKey);
                    if(toReplicate)
                        logger.info("Try to replicate key:"+kvKey+", value:"+value);
                    else
                        logger.info("Try to purge key:"+kvKey+", value:"+value);

                    success = serverConnection.put(kvKey, value, toReplicate);
                    if (!success) throw new ServerToServerErrorException("Server data is locked");

                    if (deleteKeys) delete(kvKey);
                } catch (Exception e) {
                    logger.error("Server found error finding and sending key and value to another server. Replicate = " + Boolean.toString(toReplicate), e);
                    String value = fileManager.getKV(kvKey);
                    success = reconnectAndPut(kvKey, value, toReplicate, addressToKeys.getKey());
                    if (!success) throw new ServerToServerErrorException("Server data is locked");
                }
            }
            serverConnection.disconnect();
        }
	}

    public boolean reconnectAndPut(String key, String value, boolean toReplicate, String address){
        boolean success = false;
        for (int i=0; i<5; i++) {
            try {
                serverConnection = new ServerConnection(address);
                success = serverConnection.put(key, value, toReplicate);
                if (success) break;
            } catch (Exception e) {
                logger.info("serverToServer trying to reconnect and put");
            }
        }
        return success;
    }

    // Sends a single key to all of the server's replicas - used on a client put to the server.
    // Throws exception if replication handoff is unseccussful. 
    public void serverToReplicasPut(String key) throws Exception{
        List<String> replicas = hashRing.GetReplicasByKey(key);
        if (replicas == null) return; 
        Map<String,Vector<String>> replicaKeyMap = new HashMap<String,Vector<String>>();
        Vector<String> keys = new Vector<String>();
        keys.add(key);
        for (String replica : replicas) {
            replicaKeyMap.put(replica, keys);
        }
        serverToServer(replicaKeyMap, /*toReplicate=*/true, /*deleteKeys=*/false);
    }

    // Sends all of the servers key to new replicas introduced to the ring.
    public void serverToNewReplicas() throws Exception{
        List<String> currentReplicas = hashRing.GetReplicasByKey(currentFullAddress);
        if (currentReplicas == null) return;
        logger.info("current replicas"+currentReplicas.toString()+",current address"+currentFullAddress);
        // Send off all keys on this server to the newly added replicas - with a non replicating put message.
        Map<String,Vector<String>> replicaKeyMap = new HashMap<String,Vector<String>>();
        Vector<String> keys = fileManager.getKeys();
        // Keys that dont belong to other coordinators.
        Vector<String> inRangeKeys = new Vector<String>();
        for (String key : keys) {
            if (inHashRange(key)) inRangeKeys.add(key);
        }
        logger.info("DataManager server to newrep:"+inRangeKeys.toString());
        for (String replica : currentReplicas) {
            replicaKeyMap.put(replica, inRangeKeys);
        }
        serverToServer(replicaKeyMap, /*toReplicate=*/true, /*deleteKeys=*/false);
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
