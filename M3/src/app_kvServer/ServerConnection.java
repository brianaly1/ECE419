package app_kvServer;

import org.apache.log4j.Logger;
import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.ProtoKVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class ServerConnection {
    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
    private Logger logger = Logger.getRootLogger();

    private boolean stop = false;

    private KVStore serverComms;

    private String hostname;
    private int port;

    ServerConnection(String fullAddress) {
        String[] strs = fullAddress.split(":");
        this.hostname = strs[0];
        this.port = Integer.parseInt(strs[1]);
        try {
            serverComms= new KVStore(hostname,port);
            newConnection(hostname, port);
        } catch (Exception e) {
            logger.error("Could not start server to server connection.", e);
        }
    }

    ServerConnection(String hostname, int port) {
    	this.hostname = hostname;
    	this.port = port;
        try {
            serverComms= new KVStore(hostname,port);
            newConnection(hostname, port);
        } catch (Exception e) {
            logger.error("Could not start server to server connection.", e);
        }
    }

    public void newConnection(String hostname, int port) throws Exception{
        if(serverComms.isConnected()){
            disconnect();
        }
        serverComms.setAddress(hostname,port);
        serverComms.connect();
        System.out.println("connected");

    }

    public int put(String key, String value, boolean toReplicate) throws Exception{
        KVMessage response = toReplicate ? serverComms.replicate(key,value) : serverComms.put(key, value);
        if (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE || response.getStatus() == StatusType.REPLICATE_SUCCESS) {
            return 1;
        }
        return 0;
    }

    public void disconnect() {
        if(serverComms.isConnected()) {
            serverComms.disconnect();
        }
    }
}