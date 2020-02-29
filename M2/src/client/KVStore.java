package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import ecs.ECSHashRing;
import shared.messages.KVMessage;
import org.apache.log4j.Logger;
import shared.messages.ProtoKVMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();

	private static final int MAX_KEY_SIZE = 20;
	private static final int MAX_VALUE_SIZE = 120 * 1024;

	// Network variables.
	private String addressName;
	private int portNum;
	private Socket connSocket;
	private InputStream input;
	private OutputStream output;

	// ECS variables.
	// cached_hashring is used to hold server address and its hashed value.
	private ECSHashRing cached_hashring;
	private String addressAndPortDelimiter = ":";

	public KVStore(String address, int port) {
        setAddress(address, port);
        cached_hashring = new ECSHashRing();
	}

	public void setAddress(String address, int port){
		addressName = address;
		portNum = port;
	}

	public void setAddress(String addressAndPort) {
	    String[] delimited_strings = addressAndPort.split(addressAndPortDelimiter);
	    if (delimited_strings.length != 2) {
	    	logger.error("KVStore::setAddress Something went wrong when splitting by colon.");
	    	return;
		}
	    addressName = delimited_strings[0];
	    portNum = Integer.parseInt(delimited_strings[1]);
    }

    public boolean isConnected(){
        return connSocket != null && connSocket.isConnected();
    }

	@Override
	public void connect() throws IOException {
		connSocket = new Socket(addressName, portNum);
		input = connSocket.getInputStream();
		output = connSocket.getOutputStream();
		logger.info("KVStore socket connected to: " + addressName + ":" + portNum);

		// Add this newly connected server address to locally cached hash ring.
		cached_hashring.add(addressName, portNum);
	}

	@Override
	public void disconnect() {
		if (isConnected()) {
			try{
				connSocket.close();
			} catch (IOException ioe){
				logger.error("KVStore unable to close connection!");
			}
		}
		connSocket = null;
		input = null;
		output = null;
		logger.info("KVStore socket closed.");
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
	    // Return early when there is an error.
		if (!checkKeyValueFormat(key, value)) {
			return new ProtoKVMessage(key,"",KVMessage.StatusType.PUT_ERROR);
		}

		// Send put message and try receive a response from the server currently connected.
        ProtoKVMessage putMsg = new ProtoKVMessage(key, value, KVMessage.StatusType.PUT);
        ProtoKVMessage receiveMsg = new ProtoKVMessage();
        try {
            // Check the connection and try reconnecting if necessary.
            checkConnectionAndReconnectIfNecessary(key);
            putMsg.writeMessage(output);

            // Check the received message.
            receiveMsg.parseMessage(input);
            receiveMsg = checkNotResponsible(putMsg, receiveMsg);
            logger.info("KVStore put operation done");
            return receiveMsg;

		} catch (Exception e) {
        	// Server not responding and its connection is closed.
            receiveMsg = handleShutdown(putMsg);
			if (receiveMsg != null) return receiveMsg;
			logger.info("KVStore found all servers not reachable. Disconnecting.");
            disconnect();
			throw e;
		}
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// Return early when there is an error.
		if( !checkKeyFormat(key)) {
			return new ProtoKVMessage(key,"",KVMessage.StatusType.GET_ERROR);
		}

        // Send get message and try receive a response from the server currently connected.
        ProtoKVMessage putMsg = new ProtoKVMessage(key, "", KVMessage.StatusType.GET);
        ProtoKVMessage receiveMsg = new ProtoKVMessage();
        try {
            // Check the connection and try reconnecting if necessary.
            checkConnectionAndReconnectIfNecessary(key);
            putMsg.writeMessage(output);

            // Check the received message.
			receiveMsg.parseMessage(input);
            receiveMsg = checkNotResponsible(putMsg, receiveMsg);
            logger.info("KVStore get operation done");
            return receiveMsg;

		} catch (Exception e){
            // Server not responding and its connection is closed.
            receiveMsg = handleShutdown(putMsg);
            if (receiveMsg != null) return receiveMsg;
            logger.info("KVStore found all servers not reachable.");
            disconnect();
            throw e;
        }
    }

	private boolean checkKeyValueFormat(String key, String value) {
        if (!checkKeyFormat(key)) return false;
        if (!checkValueFormat(value)) return false;
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

	private boolean checkValueFormat(String value) {
        if (value.length() > MAX_VALUE_SIZE) {
            logger.info("VALUE: " + value + " was too long");
            return false;
        }
        return true;
    }

    private ProtoKVMessage checkNotResponsible(ProtoKVMessage sentMsg, ProtoKVMessage receivedMsg) throws Exception {
        // Check if the received message contains not responsible flag.
        if (receivedMsg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // Update the cached hash ring and resend the message to the correct server
            logger.info("KVStore updating cached hash ring after receiving SERVER_NOT_RESPONSIBLE.");
            cached_hashring = new ECSHashRing(receivedMsg.getValue());
            return (ProtoKVMessage) resendMessage(sentMsg);
        }
        return receivedMsg;
    }

    private ProtoKVMessage handleShutdown(ProtoKVMessage msg) throws Exception {
        // Disconnect from the current connection and delete the node.
        logger.info("KVStore handling server abrupt shutdown.");
	    disconnect();
	    String serverAddress = cached_hashring.getNodeByKey(msg.getKey());
        cached_hashring.remove(serverAddress);
        checkConnectionAndReconnectIfNecessary(msg.getKey());
        try {
            if (!isConnected())
                return null;
            return (ProtoKVMessage) resendMessage(msg);
        } catch (Exception e) {
            return handleShutdown(msg);
        }

    }

    // Reconnect to the correct server by finding within the cached metadata, the corresponding server.
    private void checkConnectionAndReconnectIfNecessary(String key) throws Exception {
        String serverAddress = cached_hashring.getNodeByKey(key);

        // If we are connected to the correct server, return.
        if (serverAddress.equals(addressName + ":" + Integer.toString(portNum)) && isConnected())
            return;

        logger.info("KVStore reconnecting to the server in the hash range.");
        setAddress(serverAddress);
        disconnect();
        connect();
    }

    private KVMessage resendMessage(ProtoKVMessage msg) throws Exception {
	    checkConnectionAndReconnectIfNecessary(msg.getKey());
	    logger.info("KVStore resending message.");
        if (msg.getStatus() == KVMessage.StatusType.PUT)
            return this.put(msg.getKey(), msg.getValue());
        if (msg.getStatus() == KVMessage.StatusType.GET)
            return this.get(msg.getKey());

        logger.error("KVStore sending not supported message type: " + msg.getStatus());
        return null;
    }

}
