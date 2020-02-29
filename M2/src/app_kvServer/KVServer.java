package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import logger.LogSetup;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import app_kvServer.DataManager;
import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.ZkInteraction;

import server_exceptions.NotInHashRangeException;
import server_exceptions.ServerNotActiveException;
import server_exceptions.ServerDataLockedException;
import static shared.messages.KVMessage.StatusType.PUT_UPDATE;

public class KVServer implements IKVServer, Runnable {

	private static final int MAX_KEY_SIZE =20;
	private static final int MAX_VALUE_SIZE =120*1024;

	private static Logger logger = Logger.getRootLogger();

	private static ServerSocket serverSocket;

	private Boolean isDisconnected = false;
	private boolean stop =false;

	private boolean m1Trigger = true;;

	private ArrayList<Thread> childThreads;
	//private boolean stop;
	private static boolean running;
	private static int ip;
	private static int port;
	private static String zkAdress;
	private static String zkRoot;
	private static String zNodeName;
	private static int cacheSize;
	private static String strategy;
	private static CacheStrategy strategyEnum;
	private static DataManager dataManager;
	private static ZkInteraction zkInteraction;
	private String localHost="127.0.0.1";
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy, String zkAdress, String zkRoot, String zNodeName) {
		logger.setLevel(Level.ALL);
		assert(cacheSize > 0);
		assert(strategy != "None");

		Map<String, CacheStrategy> strategyMap = new HashMap<String, CacheStrategy>();
		strategyMap.put("None", CacheStrategy.None);
		strategyMap.put("LRU", CacheStrategy.LRU);
		strategyMap.put("LFU", CacheStrategy.LFU);
		strategyMap.put("FIFO", CacheStrategy.FIFO);

		this.port = port;
		this.zkAdress = zkAdress;
		this.zkRoot = zkRoot;
		this.zNodeName = zNodeName;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.strategyEnum = strategyMap.get(strategy);
		try {
			this.dataManager = new DataManager (cacheSize, strategyEnum, zNodeName, localHost+":"+port);
			this.zkInteraction = new ZkInteraction(zkAdress, zkRoot, zNodeName, dataManager,localHost+":"+port);
		} catch (Exception e) {
			logger.error("Error! Could not initialize zookeeper and data manager. \n", e);
		}
		this.m1Trigger = false;
	}

	public KVServer(int port, int cacheSize, String strategy) {
		assert(cacheSize > 0);
		assert(strategy != "None");

		Map<String, CacheStrategy> strategyMap = new HashMap<String, CacheStrategy>();
		strategyMap.put("None", CacheStrategy.None);
		strategyMap.put("LRU", CacheStrategy.LRU);
		strategyMap.put("LFU", CacheStrategy.LFU);
		strategyMap.put("FIFO", CacheStrategy.FIFO);

		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.strategyEnum = strategyMap.get(strategy);
		this.dataManager = new DataManager(cacheSize, strategyEnum, localHost+":"+port);
		this.m1Trigger = true;
	}

	@Override
	public int getPort(){
		return serverSocket.getLocalPort();
	}

	@Override
	public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return this.strategyEnum;
	}

	@Override
	public int getCacheSize(){
		return cacheSize;
	}

	@Override
	public Boolean inStorage(String key) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		try {
			return dataManager.inStorage(key);
		}catch (Exception e) {
			logger.error("Error! Key in storage could not be checked! \n", e);
			return null;
		} finally {
			boolean unlocked = unlockData();
		}
	}

	@Override
	public Boolean inCache(String key) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		try {
			return dataManager.inCache(key);
		}catch (Exception e) {
			logger.error("Error! Key in cache could not be checked! \n", e);
			return null;
		} finally {
			boolean unlocked = unlockData();
		}
	}

	public Boolean inHashRange(String key) throws Exception {
		if (m1Trigger) {
			return true;
		}
		return dataManager.inHashRange(key);
	}

	public Boolean isActive() throws Exception {
		if (m1Trigger) {
			return true;
		}
		return dataManager.isActive();
	}

	public Boolean lockData() throws Exception {
		if (m1Trigger) {
			return true;
		}
		return dataManager.getLock();
	}

	public Boolean unlockData() throws Exception {
		if (m1Trigger) {
			return true;
		}
		dataManager.releaseLock();
		return true;
	}

	public String getMetadata(){
		try {
			return zkInteraction.getRootMetadata();
		} catch (Exception e) {
			logger.error("Error! Could not retrieve metadata string! \n", e);
			return null;
		}
	}

	@Override
	public String getKV(String key) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		if(!checkKeyFormat(key)){
			boolean unlocked = unlockData();
			return "";
		}

		try {
			return dataManager.getKV(key);
		}catch (Exception e) {
			logger.error("Error! Value could not be extracted! \n", e);
			return "";
		} finally {
			boolean unlocked = unlockData();
		}

	}

	@Override
	public void putKV(String key, String value) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		if(!checkKeyValueFormat(key,value)){
			boolean unlocked = unlockData();
			return;
		}

		try {
			if(value.isEmpty()) {
				dataManager.delete(key);
			}else {
				//search for value,
				if (dataManager.inStorage(key)) {
					dataManager.update(key, value);
				} else {
					dataManager.putKV(key, value);
				}
			}
		} catch (Exception e) {
			logger.error("Error! Key-Value pair could not be inserted! \n", e);
		} finally {
			boolean unlocked = unlockData();
		}
	}


	public KVMessage.StatusType putKVThread(String key, String value) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			logger.info("is not active");
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;
		if(!checkKeyValueFormat(key,value)){
			boolean unlocked = unlockData();
			return status;
		}
		try {
			if(value.isEmpty()) {
				status = KVMessage.StatusType.DELETE_ERROR;
				dataManager.delete(key);
				status = KVMessage.StatusType.DELETE_SUCCESS;

			}else {
				//search for value,
				if (dataManager.inStorage(key)) {
					dataManager.update(key, value);
					status = KVMessage.StatusType.PUT_UPDATE;
				} else {
					dataManager.putKV(key, value);
					status = KVMessage.StatusType.PUT_SUCCESS;
				}
			}
		}catch (Exception e) {
			System.out.println("Error! Key-Value pair could not be inserted! \n");
			logger.error("Error! Key-Value pair could not be inserted! \n", e);
		} finally {
			boolean unlocked = unlockData();
		}
		return status;

	}

	private boolean checkKeyValueFormat(String key, String value){
		//check format of key/value pair
		if(key.length()>MAX_KEY_SIZE){
			logger.info("Key: "+key+" was too long");
			return false;
		}else if(key.contains(" ")){
			logger.info("Key contains white space.");
			return false;
		}else if (key.length()==0){
			logger.info("Key was empty");
			return false;
		}else if(value.length()>MAX_VALUE_SIZE) {
			logger.info("VALUE: " + value + " was too long");
			return false;
		}
		return true;
	}
	private boolean checkKeyFormat(String key){
		//check format of key/value pair
		if(key.length()>MAX_KEY_SIZE){
			logger.info("Key: "+key+" was too long");
			return false;
		}else if(key.contains(" ")){
			logger.info("Key contains white space.");
			return false;
		}else if (key.length()==0){
			logger.info("Key was empty");
			return false;
		}
		return true;
	}
	@Override
	public void clearCache() {
		try {
			dataManager.clearCache();
		} catch (Exception e) {
			logger.error("Error! Could not clear cache. \n", e);
		}
	}

	@Override
	public void clearStorage(){
		try {
			dataManager.clearStorage();
		} catch (Exception e) {
			logger.error("Error! Could not clear storage. \n", e);
		}
	}

	@Override
	public void run() {
		running = initializeServer();
		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					Runnable clientThread= new ClientConnection(this,client,logger);
					Thread child = new Thread(clientThread);
					child.start();
					childThreads.add(child);
					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	public void offloadStorage(){
		try {
			dataManager.purge(/*purgeAll=*/true);
		} catch (Exception e) {
			logger.info("Purge unsucessful");
		}
	}

	@Override
	public void kill(){
		// TODO Auto-generated method stub
		for (Iterator<Thread> iterator = childThreads.iterator(); iterator.hasNext(); ) {
			Thread value = iterator.next();
			value.interrupt();
		}
		try {
			zkInteraction.close();
		} catch (Exception e) {
			logger.info("Couldn't close the zookeeper connection, killing anyways");
		}
		stopServer();
		zkInteraction=null;
		dataManager=null;
		serverSocket=null;
		running=false;
	}

	@Override
	public void close(){
		for (Iterator<Thread> iterator = childThreads.iterator(); iterator.hasNext(); ) {
			Thread value = iterator.next();
			value.interrupt();
		}
		stopServer();
		// TODO Auto-generated method stub
		running=false;
	}

	public void delete(String key) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		try{
			dataManager.delete(key);
		} catch (Exception e) {
			logger.error("Error!, Unable to delete key! \n",e);
		} finally {
			boolean unlocked = unlockData();
		}
	}

	public void update(String key, String value) throws Exception{
		if (!lockData()) {
            throw new ServerDataLockedException("Server data is locked");
        }
		if (!isActive()) {
			boolean unlocked = unlockData();
			throw new ServerNotActiveException("Server not in root metadata");
		}
        if (!inHashRange(key)) {
        	boolean unlocked = unlockData();
            throw new NotInHashRangeException("Key not in storage range of this kvserver");
        }
		try{
			dataManager.update(key, value);

		} catch (Exception e) {
			logger.error("Error!, Unable to update key! \n",e);
		} finally {
			boolean unlocked = unlockData();
		}
	}


	private boolean isRunning() {
		return running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			childThreads = new ArrayList<>();
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		} catch (Exception e) {
			logger.error("Error!, Unable to initialize server! \n",e);
			return false;
		}
	}

	/**
	 * Main entry point for the KVServer application.
	 * @param args contains the port number at args[0]
	 * cache size at args[1], replacement policy at args[2]
	 * and log level at args[3]
	 */
	public static void main(String[] args) {
		Map<String, Level> logLevels = new HashMap<String, Level>();
		logLevels.put("ALL", Level.ALL);
		logLevels.put("DEBUG", Level.DEBUG);
		logLevels.put("INFO", Level.INFO);
		logLevels.put("WARN", Level.WARN);
		logLevels.put("ERROR", Level.ERROR);
		logLevels.put("FATAL", Level.FATAL);
		logLevels.put("OFF", Level.OFF);
		try {
			if(args.length != 7) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				String logLevelString = args[3];
				String zookeeperAdress = args[4];
				String zookeeperRoot = args[5];
				String zookeeperNodeName = args[6];
				if (logLevels.containsKey(logLevelString)) {
					new LogSetup("logs/server.log", logLevels.get(logLevelString));
				} // else need to throw exception
				new Thread(new KVServer(port, cacheSize, strategy, zookeeperAdress, zookeeperRoot, zookeeperNodeName)).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid arguments <port>! or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port>! <cacheSize>!");
			System.exit(1);
		}
	}
}
