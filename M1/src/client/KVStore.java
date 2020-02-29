package client;

import shared.messages.KVMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.ProtoKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();

	private static final int MAX_KEY_SIZE =20;
	private static final int MAX_VALUE_SIZE =120*1024;
	//socket variables
	private Socket connSocket;
	private String addressName;
	private int portNum;
	private boolean isConnected=false;
	private InputStream input;
	private OutputStream output;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		addressName=address;
		portNum=port;
	}

	public void setAddress(String addr,int port){
		addressName=addr;
		portNum=port;
	}
	@Override
	public void connect() throws IOException {
		// TODO Auto-generated method stub
		//		System.out.println("connect");

		connSocket=new Socket(addressName,portNum);
		logger.info("Socket connected to: "+addressName+":"+portNum);
		input = connSocket.getInputStream();
		output=connSocket.getOutputStream();
		isConnected=true;
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if(connSocket!=null){
			try{
				connSocket.close();
			}catch(IOException ioe){
				logger.error("Unable to close connection!");
			}
		}
		connSocket=null;
		input=null;
		output=null;
		isConnected=false;
		logger.info("Socket closed.");
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("puttestStore :"+key +"  ,  "+value);

		if(!checkKeyValueFormat(key,value)){
			return new ProtoKVMessage(key,"",KVMessage.StatusType.PUT_ERROR);
		}

		try {
			ProtoKVMessage putMsg = new ProtoKVMessage(key, value, KVMessage.StatusType.PUT);
			//        System.out.println("put write start");

			//send request
			putMsg.writeMessage(output);
			//       System.out.println("put write end");

			//receive request
			ProtoKVMessage receiveMsg = new ProtoKVMessage();
			receiveMsg.parseMessage(input);
			//        System.out.println("put receive end"+receiveMsg.toString());

			return receiveMsg;
		}catch (Exception e) {
			disconnect();
			throw e;
		}

	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		if(!checkKeyFormat(key)){
			return new ProtoKVMessage(key,"",KVMessage.StatusType.GET_ERROR);
		}

		try {
			//		        System.out.println("get write start");
//
			ProtoKVMessage putMsg = new ProtoKVMessage(key, "", KVMessage.StatusType.GET);
			putMsg.writeMessage(output);
			//receive request
			//     System.out.println("get write start");

			ProtoKVMessage receiveMsg = new ProtoKVMessage();
			receiveMsg.parseMessage(input);
			//			        System.out.println("get rev end");

			return receiveMsg;

		}catch (Exception e){
			disconnect();
			throw e;
		}
		/*String msg=receiveMsg.toString();
		System.out.println("recieved message:"+msg);*/
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

	public boolean isConnected(){
		return isConnected;
	}

}
