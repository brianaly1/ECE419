package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import app_kvServer.DataManager;
import app_kvServer.IKVServer.CacheStrategy;

import static shared.messages.KVMessage.StatusType.PUT_UPDATE;

public class KVServer implements IKVServer, Runnable {

	private static final int MAX_KEY_SIZE =20;
	private static final int MAX_VALUE_SIZE =120*1024;

	private static Logger logger = Logger.getRootLogger();

	private static ServerSocket serverSocket;

	private Boolean isDisconnected = false;
	private boolean stop =false;

	private ArrayList<Thread> childThreads;
	//private boolean stop;
	private static boolean running;
	private static int port;
	private static int cacheSize;
	private static String strategy;
	private static CacheStrategy strategyEnum;
	private static DataManager dataManager;

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
		this.dataManager = new DataManager(cacheSize, strategyEnum);
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
	public boolean inStorage(String key){
		try {
			return dataManager.inStorage(key);
		} catch (Exception e) {
			logger.error("Error! Key in storage could not be checked!", e);
			return false;
		}
	}

	@Override
	public boolean inCache(String key){
		try {
			return dataManager.inCache(key);
		} catch (Exception e) {
			logger.error("Error! Key in storage could not be checked!", e);
			return false;
		}
	}

	@Override
	public String getKV(String key) {
		System.out.println("getkv ");
		if(!checkKeyFormat(key)){
			return "";
		}

		try {
			return dataManager.getKV(key);
		} catch (Exception e) {
			logger.error("Error! Value could not be extracted!", e);
			return "";
		}

	}

	@Override
	public void putKV(String key, String value){
		System.out.println("putkvtest :"+key +"  ,  "+value);
		if(!checkKeyValueFormat(key,value)){
			return;
		}

		try {
			if(value.isEmpty()) {
				System.out.println("delete1 eb");

				dataManager.delete(key);
				System.out.println("delete eb");

			}else {
				//search for value,
				if (dataManager.inStorage(key)) {
					System.out.println("update1 eb");

					dataManager.update(key, value);
					System.out.println("update eb");

				} else {
					System.out.println("putkv1 ");

					dataManager.putKV(key, value);
					System.out.println("putkv2 eb");

				}
			}
		} catch (Exception e) {
			System.out.println("testputkv2 eb");

			logger.error("Error! Key-Value pair could not be inserted!", e);
		}
		System.out.println("putkv eb");

	}


	public KVMessage.StatusType putKVThread(String key, String value){
		KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;
		if(!checkKeyValueFormat(key,value)){
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
		} catch (Exception e) {
			logger.error("Error! Key-Value pair could not be inserted!", e);
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
		System.out.println("clearCache ");

		try {
			dataManager.clearCache();
		} catch (Exception e) {
			logger.error("Error! Could not clear cache.", e);
		}
		System.out.println("clearCache end");

	}

	@Override
	public void clearStorage(){
		System.out.println("clearStorage");
		try {
			dataManager.clearStorage();
		} catch (Exception e) {
			logger.error("Error! Could not clear storage", e);
		}
		System.out.println("clearStorage end");

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

	@Override
	public void kill(){
		// TODO Auto-generated method stub
		for (Iterator<Thread> iterator = childThreads.iterator(); iterator.hasNext(); ) {
			Thread value = iterator.next();
			value.interrupt();
		}
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

	public void delete(String key){
		try{
			dataManager.delete(key);
		} catch (Exception e) {
			logger.error("Error!, Unable to delete key! \n",e);
		}
	}

	public void update(String key, String value){
		try{
			dataManager.update(key, value);
		} catch (Exception e) {
			logger.error("Error!, Unable to update key! \n",e);
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
			if(args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				String logLevelString = args[3];
				if (logLevels.containsKey(logLevelString)) {
					new LogSetup("logs/server.log", logLevels.get(logLevelString));
				} // else need to throw exception
				new Thread(new KVServer(port, cacheSize, strategy)).start();
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
