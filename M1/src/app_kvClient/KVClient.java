package app_kvClient;

import client.KVCommInterface;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import static java.lang.Integer.parseInt;

public class KVClient implements IKVClient  {

    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
    private static final int MAX_KEY_SIZE =20;
    private static final int MAX_VALUE_SIZE =120*1024;
    private Logger logger = Logger.getRootLogger();

    private static final String PROMPT = "EchoClient> ";
    private boolean stop = false;
    private BufferedReader stdin;

    private KVStore serverComms;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        System.out.println("new Connection");
        if(serverComms.isConnected()){
            //     disconnect();
        }
        serverComms.setAddress(hostname,port);
        serverComms.connect();
        System.out.println("connected");

    }

    @Override
    public KVCommInterface getStore(){
        System.out.println("getStore");

        // TODO Auto-generated method stub
        return serverComms;
    }

    public void run() {

        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    if(serverComms.isConnected()) {
                        logger.info("Server is already connected. " +
                                "Connecting to new server.");
                        serverComms.disconnect();
                    }
                    String serverAddress = tokens[1];
                    int serverPort = parseInt(tokens[2]);
                    newConnection(serverAddress,serverPort);
                    System.out.println("Connection to server succeeded!");
                    logger.info("connected to server");
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    printError("Unknown error look at log.");
                    logger.warn("Error!", e);

                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else  if (tokens[0].equals("get")) {
            if(tokens.length == 2) {
                if(serverComms.isConnected()) {
                    if(tokens[1].length()>MAX_KEY_SIZE){
                        printError("Key sting is too long, must be less than 20 characters");
                        logger.info("Key: "+tokens[1]+" was too long");
                    }else if (tokens[1].isEmpty()){
                        printError("Key was empty.");
                        logger.info("Key was empty.");
                    }else{
                        //send request to server
                        KVMessage response=null;
                        try {
                            response=serverComms.get(tokens[1]);
                        }catch(Exception e) {
                            printError("Could not receive response from server, will disconnect from server. Please reconnect");
                            logger.info("Could not connect to server, will disconnect from server. Please reconnect",e);
                        }

                        //parse response if any was received
                        if(response!=null)
                            parseServerResponse(response);
                    }
                }else{
                    printError("Client is not connected to the server!");
                    logger.info("Client is not connected to the server!");
                }
            } else{
                printError("Command did not have the correct arguments!");
            }

        } else if(tokens[0].equals("put")){
            if(tokens.length==3||tokens.length==2){

                //check format of key/value pair
                if(tokens[1].length()>MAX_KEY_SIZE){
                    printError("Key sting is too long, must be less than 20 characters");
                    logger.info("Key: "+tokens[1]+" was too long");
                }else if(tokens[1].isEmpty()){
                    printError("Key was empty.");
                    logger.info("Key was empty.");
                }else if(tokens.length==3&&tokens[2].length()>MAX_VALUE_SIZE){
                    printError("Value sting is too long, must be less than "+MAX_VALUE_SIZE+" characters");
                    logger.info("VALUE: "+tokens[2]+" was too long");
                }else if(!serverComms.isConnected()){
                    printError("Client is not connected to the server!");
                    logger.info("Client is not connected to the server!");
                }else {
                    //send request to server
                    KVMessage response=null;
                    try {
                        if(tokens.length==2)
                            response=serverComms.put(tokens[1],"");
                        else
                            response=serverComms.put(tokens[1],tokens[2]);

                    }catch(Exception e) {
                        printError("Could not receive response from server, will disconnect from server. Please reconnect");
                        logger.error("Could not connect to server, will disconnect from server. Please reconnect",e);
                    }
                    if(response!=null)
                        parseServerResponse(response);
                }
            }else{
                printError("Command did not have the correct arguments!");
                printHelp();
            }

        } else if(tokens[0].equals("disconnect")) {
            if(serverComms.isConnected()) {
                disconnect();
                System.out.println(PROMPT +
                        "Disconnected from server. Please Connect to new server.");
            }else{
                System.out.println(PROMPT +
                        "Was not connected to a server");
            }
        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void disconnect() {
        serverComms.disconnect();
    }


    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t -Inserts a key-value pair into the storage sever or updates if key exists.\n" +
                PROMPT+"\t\t\t\t -Deletes a key-value pair if <value> is null(empty) \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the server \n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }


    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }


    /*
    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            System.out.println(msg.getMsg());
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }*/
    private void parseServerResponse(KVMessage response){
        switch (response.getStatus()){
            case GET:
                System.out.println("Response from server was wrong, sent status of GET");
                logger.warn("Response from server was wrong, sent status of GET"+response.toString());
                break;
            case PUT:
                System.out.println("Response from server was wrong, sent status of PUT");
                logger.warn("Response from server was wrong, sent status of PUT"+response.toString());
                break;
            case GET_ERROR:
                System.out.println("Key ("+response.getKey()+") was not found, could not get value.");
                logger.warn("Key was not found, could not get value."+response.toString());
                break;
            case PUT_ERROR:
                System.out.println("There was an error in PUT request, could not insert value.");
                logger.warn("There was an error in PUT request, could not insert value. Response:"+response.toString());
                break;
            case DELETE_ERROR:
                System.out.println("Delete error, did not find key. Could not delete Key: ("+response.getKey()+")");
                logger.warn("Delete error, did not find key. Could not delete Key: ("+response.getKey()+")");
                break;
            case PUT_UPDATE:
                System.out.println("Update successful. K/V pair updated: ("+response.getKey()+","+response.getValue()+")");
                logger.info("Update successful. K/V pair updated: ("+response.getKey()+","+response.getValue()+")");
                break;
            case GET_SUCCESS:
                System.out.println("Get successful. K/V pair: ("+response.getKey()+","+response.getValue()+")");
                logger.info("Get successful. K/V pair: ("+response.getKey()+","+response.getValue()+")");
                break;
            case PUT_SUCCESS:
                System.out.println("Put successful. K/V pair inserted: ("+response.getKey()+","+response.getValue()+")");
                logger.info("Put successful. K/V pair inserted: ("+response.getKey()+","+response.getValue()+")");
                break;

            case DELETE_SUCCESS:
                System.out.println("Delete successful. Deleted Key: ("+response.getKey()+")");
                logger.info("Delete successful. Deleted Key: ("+response.getKey()+")");
                break;
            default:
                System.out.println("Did not recognize status type: "+response.getStatus());
                logger.info("Did not recognize status type: "+response.getStatus());
                break;
        }
    }


    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public KVClient(){
        try {
            new LogSetup("logs/client.log", Level.OFF);
        }catch (IOException e){
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("KVClient Started!");
        serverComms= new KVStore("localhost",5000);
    }

    /**
     * Main entry point for the client application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {

        if(args.length==0) {
            KVClient app = new KVClient();
            app.run();
        }else{
            System.out.println("Invalid arguments. No arguments are required");
        }

    }


}
