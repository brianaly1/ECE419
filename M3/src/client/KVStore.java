package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Random;

import ecs.ECSHashRing;
//import jdk.dynalink.Operation;
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

	// Others
    Random rand = new Random();

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

		logger.info("KVStore socket closed. Addr:"+addressName+", port:"+portNum);
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
            System.out.println("Sherwin put started");
            checkConnectionAndReconnectIfNecessary(key, OperationType.PUT);
            putMsg.writeMessage(output);

            // Check the received message.
            receiveMsg.parseMessage(input);
            receiveMsg = checkNotResponsible(putMsg, receiveMsg);
            logger.info("KVStore put operation done");
            System.out.println("KVStore put operation done");
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

	public KVMessage replicate(String key, String value) throws Exception {
	    // Return early when there is an error.
		if (!checkKeyValueFormat(key, value)) {
			return new ProtoKVMessage(key,"",KVMessage.StatusType.REPLICATE_ERROR);
		}

		// Send replication message and try receive a response from the server currently connected.
        ProtoKVMessage putMsg = new ProtoKVMessage(key, value, KVMessage.StatusType.REPLICATE);
        ProtoKVMessage receiveMsg = new ProtoKVMessage();
        try {
            logger.info("KVStore replicate operation started");
            putMsg.writeMessage(output);

            logger.info("finish write message Replicate kvstore");
            // Check the received message.
            receiveMsg.parseMessage(input);
            logger.info("KVStore replicate operation done");
            return receiveMsg;

		} catch (Exception e) {
        	// Server not responding and its connection is closed.
            logger.error("replicate exception",e);
            receiveMsg = handleShutdown(putMsg);
			if (receiveMsg != null) return receiveMsg;
			logger.info("KVStore could not reach replica server. Disconnecting.");
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
            checkConnectionAndReconnectIfNecessary(key, OperationType.GET);
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
		System.out.println("checking not responsible server"+portNum);
        if (receivedMsg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			System.out.println("Server is not responsible");
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
        checkConnectionAndReconnectIfNecessary(msg.getKey(), OperationType.PUT);
        try {
            if (!isConnected())
                return null;
            return (ProtoKVMessage) resendMessage(msg);
        } catch (Exception e) {
            return handleShutdown(msg);
        }

    }

    // Reconnect to the correct server by finding within the cached metadata, the corresponding server.
    private void checkConnectionAndReconnectIfNecessary(String key, OperationType operationType) throws Exception {
        String serverAddress = null;

        // Find the addresses for the coordinator for the key range and the replicas.
        String coordinator = cached_hashring.getNodeByKey(key);
        if(cached_hashring.cached_map.isEmpty())
			logger.info("hashring is empty");
        else
        	logger.info("hashring is not empty");
       	logger.info("check connection coordinator:"+coordinator);
        // If we are connected to the correct server, return.
        if (isConnectedTo(coordinator))
            return;

        // If the OperationType is PUT connect to the coordinator server, otherwise it's GET, thus, connect to the
        // replica server.
        if (operationType == OperationType.PUT) {
            serverAddress = coordinator;

        } else {
            List<String> replicas = cached_hashring.GetReplicasByKey(key);
			if(replicas!=null)
            	logger.info("replicas:"+replicas.toString());
            if (replicas == null || replicas.isEmpty()) {
                serverAddress = coordinator;
            } else {
                // Check if there is any replica that is currently connected.
                for (String replica : replicas) {
                    if (isConnectedTo(replica)) return;
                }
                serverAddress = replicas.get(rand.nextInt(replicas.size()));
            }
        }

        logger.info("KVStore reconnecting to the server in the hash range.");
        setAddress(serverAddress);
        disconnect();
        connect();
    }

    public boolean isConnectedTo(String fullAddress) {
        return fullAddress.equals(addressName + ":" + Integer.toString(portNum)) && isConnected();
    }

    private KVMessage resendMessage(ProtoKVMessage msg) throws Exception {
	    logger.info("KVStore resending message.");
        if (msg.getStatus() == KVMessage.StatusType.PUT) {
            checkConnectionAndReconnectIfNecessary(msg.getKey(), OperationType.PUT);
            return this.put(msg.getKey(), msg.getValue());
        }
        if (msg.getStatus() == KVMessage.StatusType.GET) {
            checkConnectionAndReconnectIfNecessary(msg.getKey(), OperationType.GET);
            return this.get(msg.getKey());
        }

        logger.error("KVStore sending not supported message type: " + msg.getStatus());
        return null;
    }

}
