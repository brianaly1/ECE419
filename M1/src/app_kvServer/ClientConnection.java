package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientConnection implements Runnable {
    private static Logger logger;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private Boolean isDisconnected = false;
    private boolean stop =false;

    //private boolean stop;
    private static boolean running;
    private static int port;
    private static int cacheSize;
    private static String strategy;
    private static IKVServer.CacheStrategy strategyEnum;
    private static KVServer server;
    ClientConnection(KVServer manager,Socket client,Logger log){
        server=manager;
        clientSocket=client;
        logger=log;
    }

    @Override
    public void run() {
        // send connection message, read messages
        // call relevant function according to
        // request, and reply with results.

        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

        }catch(Exception e){
            logger.info("could not get streams",e);
        }
        ProtoKVMessage receiveMsg;

        long time = System.nanoTime();
        while (!stop&&clientSocket.isConnected()&&!Thread.interrupted() && !isDisconnected){

            // This is to detect if server is kept alive in AutoTest.
            if ((System.nanoTime() - time)/1000000 > 50) {
                time = System.nanoTime();
                System.out.println("ClientConnection: run while loop serving port:" + clientSocket.getPort());
            }
            receiveMsg = new ProtoKVMessage();
            try {
                receiveMsg.parseMessage(input);

            }catch(Exception e){
                stop=true;
                System.out.println("Exception while parsing mesSystem.out.println(\"ClientConnection:run\");sage, most likely a disconnect. Disconnecting from Client");
                logger.error("Exception while parsing message, most likely a disconnect. Disconnecting from Client",e);
                isDisconnected=true;
            }

            if(isDisconnected) {
                continue;
            } else if(receiveMsg.getProtobuf()==null){
                stop=true;
                continue;
            }

            try {
                ProtoKVMessage returnMsg;
                switch (receiveMsg.getStatus()) {
                    case PUT:
                        KVMessage.StatusType status = server.putKVThread(receiveMsg.getKey(), receiveMsg.getValue());
                        returnMsg =new ProtoKVMessage(receiveMsg.getKey(),receiveMsg.getValue(),status);
                        try {
                            returnMsg.writeMessage(output);
                        }catch (IOException e){
                            stop=true;
                            isDisconnected=true;
                            logger.error("Could not write to client, Will disconnect from client",e);
                        }

                        break;
                    case GET:
                        String value = server.getKV(receiveMsg.getKey());
                        if(value!=null&&!value.isEmpty()) {
                            returnMsg = new ProtoKVMessage(receiveMsg.getKey(), value, KVMessage.StatusType.GET_SUCCESS);
                            returnMsg.writeMessage(output);
                            System.out.println("Get success - Key:"+receiveMsg.getKey()+" , Value: " + value);
                        }else {
                            returnMsg = new ProtoKVMessage(receiveMsg.getKey(), "", KVMessage.StatusType.GET_ERROR);
                            returnMsg.writeMessage(output);
                            System.out.println("Value is NULL, Get not success");
                        }
                        break;
                    default:
                        System.out.println("Status Code was wrong : "+receiveMsg.getStatus());
                        logger.warn("Status Code was wrong : "+receiveMsg.getStatus());
                }

            }catch (Exception e) {

                ProtoKVMessage returnMsg ;
                if(clientSocket.isClosed()||receiveMsg.getProtobuf()==null) {
                    // stop=true;
                    //  System.out.println("Client disconnected. Closing Socket");
                    // logger.info("Client disconnected. Closing Socket");
                    continue;
                }
                try{
                    switch (receiveMsg.getStatus()) {
                        case GET:
                            System.out.println("Error getting key:" + receiveMsg.getKey());
                            logger.error("Error getting key:" + receiveMsg.getKey(), e);
                            returnMsg = new ProtoKVMessage("", "", KVMessage.StatusType.GET_ERROR);
                            returnMsg.writeMessage(output);
                            break;
                        case PUT:
                            System.out.println("Error Putting key:" + receiveMsg.getKey() + " , " + receiveMsg.getValue());
                            logger.error("Error Putting key:" + receiveMsg.getKey() + " , " + receiveMsg.getValue(), e);
                            returnMsg = new ProtoKVMessage("", "", KVMessage.StatusType.PUT_ERROR);
                            returnMsg.writeMessage(output);
                            break;
                    }
                }catch (IOException ioExcept){
                    stop=true;
                    isDisconnected=true;
                    logger.error("Could not write to client, Will disconnect from client",ioExcept);
                }
            }
        }
        try {
            clientSocket.close();
            System.out.println("KVServer:Client connection closed:" + clientSocket.getInetAddress().getHostName()
                    + " on port " + clientSocket.getPort());
            logger.info("Client connection closed:" + clientSocket.getInetAddress().getHostName()
                    + " on port " + clientSocket.getPort());
        }catch (Exception e){
            System.out.println("KVServer: Could not close client connection:" + clientSocket.getInetAddress().getHostName()
                    + " on port " + clientSocket.getPort());
            logger.info("Client connection closed:" + clientSocket.getInetAddress().getHostName()
                    + " on port " + clientSocket.getPort());
        }
        System.out.println("ClientConnection:run:Ends");
    }
}

