package ecs;


import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ECSNode implements IECSNode{
    private String name;
    private String hostName;
    private int port;
    private String[] hashRange;
    private boolean started;

    private String zkHostName;
    private String zkPort;
    private Socket connSocket;
    private InputStream input;
    private OutputStream output;
    Process proc;
    String script = "sshscript.sh";
    String scriptWin = "sshscript.bat";

    public ECSNode(String serverName,String serverHostName,int portNum){
        name=serverName;
        hostName=serverHostName;
        port=portNum;
    }
    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName(){
        return name;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost(){
        return hostName;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort(){
        return port;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange(){
        return hashRange;
    }
    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeHashRange(String start,String end){
        hashRange=new String[]{start,end};
    }

    public void setZKInfo(String host,String portNum){
        zkHostName=host;
        zkPort=portNum;

    }

    public void connectToServer()throws Exception{
        try {
            System.out.println("connectToServer");

            connSocket = new Socket(hostName, port);
            System.out.println("connectToServersocket");

            input = connSocket.getInputStream();
            System.out.println("connectToServerstrea");

            output = connSocket.getOutputStream();
            System.out.println("connectToServerstrea,");

        }catch(Exception e){
            throw e;
        }
    }

    public void start(){
        ProtoKVMessage putMsg = new ProtoKVMessage("admin", "test", KVMessage.StatusType.SERVER_START);
        try {
            putMsg.writeMessage(output);
            ProtoKVMessage receiveMsg = new ProtoKVMessage();
            receiveMsg.parseMessage(input);
        }catch (Exception e){
            System.out.println("Could not start server");
        }
    }

    public void stop(){
        ProtoKVMessage putMsg = new ProtoKVMessage("admin", "test", KVMessage.StatusType.SERVER_STOP);
        try {
            putMsg.writeMessage(output);
            ProtoKVMessage receiveMsg = new ProtoKVMessage();
            receiveMsg.parseMessage(input);
        }catch (Exception e){
            System.out.println("Could not stop server");
        }
    }
    public void shutDown(){
        ProtoKVMessage putMsg = new ProtoKVMessage("admin", "test", KVMessage.StatusType.SERVER_SHUTDOWN);
        try {
            putMsg.writeMessage(output);
           // ProtoKVMessage receiveMsg = new ProtoKVMessage();
           // receiveMsg.parseMessage(input);
        }catch (Exception e){
            System.out.println("Could not shutdown server cleanly");
        }
        started=false;
    }
    public void killNode(){
        ProtoKVMessage putMsg = new ProtoKVMessage("admin", "test", KVMessage.StatusType.SERVER_KILL);
        try {

            putMsg.writeMessage(output);
            // ProtoKVMessage receiveMsg = new ProtoKVMessage();
            // receiveMsg.parseMessage(input);
        }catch (Exception e){
            System.out.println("Could not shutdown server cleanly");
        }
        started=false;
    }
    public boolean startUpAck(){
        ProtoKVMessage putMsg = new ProtoKVMessage("admin", "test", KVMessage.StatusType.SETUP_ACK);
        try {
            putMsg.writeMessage(output);
            ProtoKVMessage receiveMsg = new ProtoKVMessage();
            receiveMsg.parseMessage(input);
            if(receiveMsg.getStatus()== KVMessage.StatusType.SETUP_ACK){
                return true;
            }else{
                return false;
            }
        }catch (Exception e){
            //System.out.println("Could not setup server");
            return false;
        }
    }

    public byte[] getNodeMetaData(){
        String temp=name+","+hostName+","+port;
        return temp.getBytes();
    }

    public boolean sshServerStart(String replacementPolicy,int cacheSize){
        Runtime run = Runtime.getRuntime();
        System.out.println("startsshServer");
        try {
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                proc = run.exec("cmd /c " + "ssh -n herna130@127.0.0.1 nohup java -jar ./ECE4/ECE419/M2/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name);
                String sent="ssh -n herna130@127.0.0.1 nohup java -jar ./ECE4/ECE419/M2/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name;
                System.out.println(sent);

                //proc = run.exec(scriptWin);
            } else {
                //String sent="ssh -n brianaly@localhost nohup java -jar ./ECE419/M3/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name+" &";
                String sent="ssh -n herna130@localhost nohup java -jar ./ECEFourthYear/ECE419/M3/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name+" &";
                System.out.println(sent);
                //proc = run.exec("ssh -o StrictHostKeyChecking=no -n brianaly@localhost nohup java -jar ./ECE419/M3/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name+" &");
                proc = run.exec("ssh -o StrictHostKeyChecking=no -n herna130@localhost nohup java -jar ./ECEFourthYear/ECE419/M3/m2-server.jar "+port+" "+cacheSize+" "+replacementPolicy+" ALL "+ zkHostName + ":" + zkPort+" /ECE419 "+name+" &");
            }
            System.out.println("testScript");
        } catch (IOException e) {
            e.printStackTrace();
            started=false;
            return false;
        }
        started=true;
        return true;
    }
    public boolean isStarted(){
        return started;
    }

}
