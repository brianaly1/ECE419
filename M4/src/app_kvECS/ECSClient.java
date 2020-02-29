package app_kvECS;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ecs.ECSNode;
import ecs.ECSServerWatcher;
import ecs.IECSNode;
import jdk.nashorn.internal.runtime.ECMAException;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import shared.messages.KVMessage;

import static java.lang.Integer.parseInt;


public class ECSClient implements IECSClient,Runnable {
    private ZooKeeper zkManager;
    private String zkRoot="/ECE419";
    String localHost = "127.0.0.1";
    String portNum="4321";
    public Collection<ECSNode> inactiveServerNodes;
    public Collection<ECSNode> activeServerNodes;
    public Collection<ECSNode> aliveServerNodes;
    private boolean stop=false;
    private static final String PROMPT = "ECSClient> ";
    private Logger logger = Logger.getRootLogger();
    private BufferedReader stdin;

    @Override
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

        if (tokens[0].equals("addNodes")){
            if(tokens.length == 4) {
                int numNodes=parseInt(tokens[1]);
                String replacementStrat=tokens[2];
                int cacheSize=parseInt(tokens[3]);

                Collection<IECSNode> nodes=addNodes(numNodes,replacementStrat,cacheSize);
                logger.info("added nodes:");
                for(IECSNode node:nodes){
                    logger.info(node.getNodeMetaData().toString());
                }
            }else{
                printError("Invalid number of parameters!");
            }
        } else  if (tokens[0].equals("start")) {
            if(tokens.length==1){
                start();
            }
            else{
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("stop")){
            if(tokens.length==1){
                if(stop()){
                    logger.info("Was able to successfully stop servers");
                }
            }
            else{
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("shutDown")) {
            if(tokens.length==1){
                if(shutdown()){
                    logger.info("Was able to successfully shutdown servers");
                }
            }
            else{
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("addNode")) {
            if(tokens.length == 3) {
                String replacementStrat=tokens[1];
                int cacheSize=parseInt(tokens[2]);
                IECSNode node =addNode(replacementStrat,cacheSize);
                if(node!=null) {
                    logger.info("added nodes:");
                    logger.info(node.getNodeMetaData().toString());
                }else{
                    logger.info("Could not add node");
                }
            }else{
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("removeNode")) {
            if(tokens.length==2){
                Collection<String> namesToRemove=new ArrayList<>();
                namesToRemove.add(tokens[1]);
                removeNodes(namesToRemove);
            }else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }



    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("addNodes <numNodes> <replacementStrategy> <cacheSize>");
        sb.append("\t adds n nodes\n");

        sb.append(PROMPT).append("start");
        sb.append("\t\t\t starts storage service\n");

        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops all server instances but the remote processes are still running\n");

        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t Stops all server instances and exits the remote processes\n");

        sb.append(PROMPT).append("addNode <replacementStrategy> <cacheSize>");
        sb.append("\t adds 1 node\n");

        sb.append(PROMPT).append("removeNode <serverName>");
        sb.append("\t removes node with name<serverName> which triggers data handoff to other servers\n");
        System.out.println(sb.toString());
    }


    @Override
    public boolean start() {
        // TODO
        Iterator<ECSNode> iter = aliveServerNodes.iterator();
        logger.info("Starting servers");
        while(iter.hasNext()){
            ECSNode ecs = iter.next();
            //ecs.start();
            iter.remove();
            activeServerNodes.add(ecs);
            logger.info("Added server to hash ring:"+ecs.getNodeName()+", port:"+ecs.getNodePort());
        }
        try {
            zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
        }catch (Exception e){
            logger.error("Could not update zookeeper",e);
        }
        return true;
    }

    @Override
    public boolean stop() {
        // TODO
        Iterator<ECSNode> iter = activeServerNodes.iterator();

        while(iter.hasNext()){
            ECSNode ecs = iter.next();
            //ecs.stop();
            iter.remove();
            aliveServerNodes.add(ecs);
            logger.info("Removed server from hash ring:"+ecs.getNodeName());
        }
        try {
            zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
        }catch (Exception e){
            logger.error("Could not update zookeeper",e);
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        // TODO
        Iterator<ECSNode> iter = activeServerNodes.iterator();
        ECSNode primaryNode =null;
        if(iter.hasNext()){
            primaryNode = iter.next();
        }
        List<ECSNode> nodesToShutDownFirst=new ArrayList<>();

        while(iter.hasNext()){
            ECSNode ecs = iter.next();
            nodesToShutDownFirst.add(ecs);
            iter.remove();
            inactiveServerNodes.add(ecs);
            logger.info("Removed and shutdown server in hash ring:"+ecs.getNodeName());
        }
        iter = aliveServerNodes.iterator();

        while(iter.hasNext()){
            ECSNode ecs = iter.next();
            nodesToShutDownFirst.add(ecs);
            iter.remove();
            inactiveServerNodes.add(ecs);
            logger.info("Shutdown server:"+ecs.getNodeName());
        }
        try {
            zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
        }catch (Exception e){
            logger.error("Could not update zookeeper",e);
        }
        try {
            for (ECSNode node : nodesToShutDownFirst) {
                node.killNode();
            }
            TimeUnit.MILLISECONDS.sleep(3000);
            zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
            activeServerNodes.remove(primaryNode);


            primaryNode.shutDown();
            List<ECSNode> nodes = new ArrayList<>(inactiveServerNodes);
            inactiveServerNodes.clear();
            inactiveServerNodes.add(primaryNode);
            inactiveServerNodes.addAll(nodes);
        }catch (Exception e){
            logger.info("could not killnodes cleanly");
        }

        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {

        Collection<IECSNode> nodes=setupNodes(1,cacheStrategy,cacheSize);

        for(IECSNode node:nodes){
            try{
                logger.info("Started a server:"+node.getNodeName());
                return node;
            }catch (Exception e){
                logger.error("Could not add znode",e);
            }
        }
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        Collection<IECSNode> nodes=setupNodes(count,cacheStrategy,cacheSize);
        for(IECSNode node:nodes){
            try{
                logger.info("Started a server:"+node.getNodeName());

            }catch (Exception e){
                logger.error("Could not add znode",e);
            }
        }

        return nodes;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        Clock timer= Clock.systemUTC();

        Collection<IECSNode> nodes=new ArrayList<>();
        System.out.println("setupNodes");
        if(inactiveServerNodes.size()<count) {
            return nodes;
        }else{
            for(int i=0;i<count;i++) {
                Iterator<ECSNode> iter = inactiveServerNodes.iterator();
                ECSNode node = iter.next();
                try {
                    ((ECSNode) node).sshServerStart(cacheStrategy,cacheSize);
                    TimeUnit.MILLISECONDS.sleep(2000);
                    ((ECSNode) node).connectToServer();
                } catch (Exception e) {
                    System.out.println("Could not connect to server");
                    logger.error("Could not connect to server", e);
                }
                boolean isSetup=false;
                Instant start = timer.instant();
                long between=0;
                System.out.println("while");
                while(!isSetup&&between<4) {
                    isSetup= node.startUpAck();
                    Instant end = timer.instant();
                    between = Duration.between(start, end).getSeconds();
                    //System.out.println("between"+between);
                }
                if(isSetup){
                    System.out.println("is setup");
                    iter.remove();
                    nodes.add(node);
                    aliveServerNodes.add((ECSNode) node);

                }else{
                    System.out.println("isnot setup");
                }
            }
        }
        return nodes;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO

        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        return killNodes(nodeNames);
    }
    private boolean killNodes(Collection<String> nodeNames) {
        // TODO
        for(String name: nodeNames){
            ECSNode ecs = activeServerNodes.stream()
                    .filter(node -> name.matches(node.getNodeName()))
                    .findFirst()
                    .orElse(null);

            if(ecs!=null) {
                activeServerNodes.remove(ecs);
                inactiveServerNodes.add(ecs);
                try {
                    zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
                }catch (Exception e){
                    logger.error("Could not update zookeeper",e);
                }
                ecs.killNode();
                logger.info("Removed server from hash ring:"+ecs.getNodeName());
            }else{
                ecs = aliveServerNodes.stream()
                        .filter(node -> name.matches(node.getNodeName()))
                        .findFirst()
                        .orElse(null);

                if(ecs!=null) {
                    inactiveServerNodes.add(ecs);
                    aliveServerNodes.remove(ecs);
                    try {
                        zkManager.setData(zkRoot, getRootMetaData(activeServerNodes), -1);
                    }catch (Exception e){
                        logger.error("Could not update zookeeper",e);
                    }
                    logger.info("Removed server from hash ring:"+ecs.getNodeName());
                    ecs.killNode();
                }else{
                    logger.info("Could not find  server"+name);
                }
            }
        }

        return true;
    }
    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        Map<String,IECSNode> mapOfNodes=new HashMap<>();
        for(ECSNode ecs:activeServerNodes){
            mapOfNodes.put(ecs.getNodeName(),ecs);
        }
        for(ECSNode ecs:aliveServerNodes){
            mapOfNodes.put(ecs.getNodeName(),ecs);
        }
        return mapOfNodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
       /*String hash = computeSingleHash(Key);
        for(ECSNode node:activeServerNodes){
            String[] hashRange=node.getNodeHashRange();
            String start=hashRange[0];
            String end = hashRange[1];
            if(hash.compareTo(start)>0&&hash.compareTo(end)<=0){
                return node;
            }
        }*/
        return null;
    }

    public static void main(String[] args) {
        // TODO
        int clientPort = 4321; // not standard
        int numConnections = 10;
        int tickTime = 2000;

        File dir = new File("./conf/zooconfig").getAbsoluteFile();
        try {
            ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
            ServerCnxnFactory factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress(clientPort), numConnections);
            factory.startup(server); // start the server.
        }catch (Exception e){
            System.out.println("could not start zookeeper server");
            e.printStackTrace();
            return;
        }

        try {
            if(args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: <config path>!");
            } else {
                Collection<ECSNode> nodes = parseConfigFile(args[0]);
                new LogSetup("logs/ecs.log", Level.ALL);

                ECSClient app = new ECSClient(nodes);
                app.run();
            }
        } catch (Exception e) {
            System.out.println("Error! Unable to Start ECS!");
            e.printStackTrace();
            System.exit(1);
        }


    }

    private ECSClient(Collection<ECSNode> nodes){
        logger.setLevel(Level.INFO);
        inactiveServerNodes=nodes;
        aliveServerNodes=new ArrayList<>();
        activeServerNodes=new ArrayList<>();
        CountDownLatch sig = new CountDownLatch(0);
        /*try {
            localHost= InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Unable to get ip, setting ip as localhost(127.0.0.1)");
        }*/
        String zkAddress=localHost+":"+portNum;
        logger.warn("zkAddress is:"+zkAddress);
        try {
            zkManager = new ZooKeeper(zkAddress, 2000, event -> {
                if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                    // connection fully established can proceed
                    sig.countDown();
                }
            });
        }catch (Exception e){
            logger.error("Could not start zookeeper",e);
        }
        try {
            sig.await();
        } catch (InterruptedException e) {
            // Should never happen
            logger.error("could not start zookeeper",e);
        }
        try {
            if (zkManager.exists(zkRoot, false) == null) {
                zkManager.create(zkRoot, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zkManager.setData(zkRoot,"".getBytes(),-1);
        }catch (Exception e){
            logger.error("could not add zookeeper root node",e);
        }
        ECSServerWatcher serverWatcher=new ECSServerWatcher(this,zkManager,zkRoot);
        try {
            zkManager.getChildren(zkRoot, serverWatcher);
        }catch (Exception e){
            logger.error("could not watch for children nodes",e);
        }
        for(ECSNode node:nodes){
            node.setZKInfo(localHost,portNum);
        }
        logger.setLevel(Level.INFO);

        logger.info("ECS started with ZooKeeper " + localHost+":"+portNum);

    }


    private static Collection<ECSNode> parseConfigFile(String fileName){
        ArrayList<ECSNode> nodeArray= new ArrayList<>();
        try {
            FileReader in = new FileReader(fileName);
            BufferedReader br = new BufferedReader(in);
            String newLine;
            while ((newLine=br.readLine())!= null) {
                String[] tokens = newLine.split("\\s+");
                ECSNode newNode=new ECSNode(tokens[0],tokens[1],parseInt(tokens[2]));
                nodeArray.add(newNode);
            }
            in.close();

        }catch (Exception e){

            e.printStackTrace();
            return null;
        }
        return nodeArray;
    }

    private byte[] getRootMetaData(Collection<ECSNode> nodes){
        Map<String,String> metaData=new HashMap<>();
        for(ECSNode node:nodes){
            metaData.put(node.getNodeName(),node.getNodeHost()+":"+node.getNodePort());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String mapString="";
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(metaData);
            oos.close();
            mapString=Base64.getEncoder().encodeToString(baos.toByteArray());
        }catch(Exception e){
            logger.error("could not get metaData",e);
        }
        System.out.println("test:"+mapString);
        return mapString.getBytes();
    }

    private String getNodePath(IECSNode node){
        return zkRoot+"/"+node.getNodeName();
    }
}
