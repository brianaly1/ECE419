package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import server_exceptions.NotInHashRangeException;
import server_exceptions.ServerNotActiveException;
import server_exceptions.ServerDataLockedException;

public class ClientConnection implements Runnable {
    private static Logger logger;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private Boolean isDisconnected = false;
    private boolean stop =false;

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
                        KVMessage.StatusType putStatus = server.putKVThread(receiveMsg.getKey(), receiveMsg.getValue(), /*isReplicationPut=*/false);
                        KVMessage.StatusType replicationStatus = server.sendToReplicas(receiveMsg.getKey(), receiveMsg.getValue());
                        KVMessage.StatusType stat = KVMessage.StatusType.PUT_ERROR;

                        if (putStatus == KVMessage.StatusType.PUT_SUCCESS && replicationStatus == KVMessage.StatusType.REPLICATE_SUCCESS) {
                            stat = KVMessage.StatusType.PUT_SUCCESS;
                        }

                        returnMsg =new ProtoKVMessage(receiveMsg.getKey(),receiveMsg.getValue(),stat);

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

                    case REPLICATE:
                        logger.info("handling replicate request, storing a coordinators kv pair");
                        KVMessage.StatusType status = server.putKVThread(receiveMsg.getKey(), receiveMsg.getValue(), /*isReplicationPut=*/true);
                        logger.info("finished replicate request handling");
                        returnMsg =new ProtoKVMessage(receiveMsg.getKey(),receiveMsg.getValue(),status);
                        try {
                            returnMsg.writeMessage(output);
                            logger.info("finish writemessage replicate client connection");
                        }catch (IOException e){
                            stop=true;
                            isDisconnected=true;
                            logger.error("Could not store a replication range key value pair, will disconnect",e);
                        }
                        break;

                    case SETUP_ACK:
                        returnMsg = new ProtoKVMessage(receiveMsg.getKey(), receiveMsg.getValue(), KVMessage.StatusType.SETUP_ACK);
                        returnMsg.writeMessage(output);
                        System.out.println("Sent ack back to ECS Client");
                        break;

                    case SERVER_KILL:
                        System.out.println("Offloading storage ...");
                        server.offloadStorage();
                        server.kill();
                        System.out.println("Shutdown server ...");
                        stop=true;
                        break;

                    case SERVER_SHUTDOWN:
                        System.out.println("Shutdown server from ecs...");
                        server.kill();
                        System.out.println("Shutdown server...");
                        stop=true;
                        break;

                    default:
                        System.out.println("Status Code was wrong : "+receiveMsg.getStatus());
                        logger.warn("Status Code was wrong : "+receiveMsg.getStatus());
                }

            } catch (NotInHashRangeException he) {
                try {
                    logger.info("Sending server not responsible message ...");
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
                    ProtoKVMessage returnMsg = new ProtoKVMessage("", server.getMetadata(), status);
                    returnMsg.writeMessage(output);
                } catch (IOException eio) {
                    stop=true;
                    isDisconnected=true;
                    logger.error("Could not write to client, Will disconnect from client",eio);
                }
                
            } catch (ServerNotActiveException ae) {
                try {
                    logger.info("Sending server not active message ...");
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_STOPPED;
                    ProtoKVMessage returnMsg = new ProtoKVMessage("", "", status);
                    returnMsg.writeMessage(output);
                } catch (IOException eio) {
                    stop=true;
                    isDisconnected=true;
                    logger.error("Could not write to client, Will disconnect from client",eio);
                }
                
            } catch (ServerDataLockedException le) {
                try {
                    logger.info("Sending server locked message ...");
                    KVMessage.StatusType status = KVMessage.StatusType.SERVER_WRITE_LOCK;
                    ProtoKVMessage returnMsg = new ProtoKVMessage("", "", status);
                    returnMsg.writeMessage(output);
                } catch (IOException eio) {
                    stop=true;
                    isDisconnected=true;
                    logger.error("Could not write to client, Will disconnect from client",eio);
                }
                
            } catch (Exception e) {

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
                        case REPLICATE:
                            System.out.println("Error replicating key:" + receiveMsg.getKey() + " , " + receiveMsg.getValue());
                            logger.error("Error replicating key:" + receiveMsg.getKey() + " , " + receiveMsg.getValue(), e);
                            returnMsg = new ProtoKVMessage("", "", KVMessage.StatusType.REPLICATE_ERROR);
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

