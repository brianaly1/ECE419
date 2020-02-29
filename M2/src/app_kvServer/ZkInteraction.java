package app_kvServer;

import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import app_kvServer.DataManager;
import app_kvServer.HashDataMonitor;


public class ZkInteraction {

    private static Logger logger = Logger.getRootLogger();
    private ZooKeeper zk;
    private boolean connected;
    private String adress;
    private String root;
    private HashDataMonitor rootDataMonitor;
    private String nodeName;
    private DataManager dataManager;
    
    public ZkInteraction(String zkadress, String root, String nodeName, DataManager dataManager,String serverAddr) throws Exception {
        this.adress = serverAddr;
        this.root = root;
        this.nodeName = nodeName;
        this.dataManager = dataManager;
        this.connected = false;
        try {
            connect(zkadress);
            initRootMetadata(root);
        }catch (Exception e){
            logger.error("Could not start zkinteraction",e);
        }
    }
    public void connect(String host) throws Exception {
        logger.info("Initializing zookeeper connection ...");
        CountDownLatch connSignal = new CountDownLatch(1);
        zk = new ZooKeeper(host, /*timeOutSeconds=*/5000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
        connected = true;
        logger.info("Zookeeper connection established ...");
    }

    public void initRootMetadata(String root) throws Exception{
        logger.info("Retrieveing root metadata from ZK server ...");
        Stat status = new Stat();
        // Initialize hashDataMonitor
	    byte[] rootMetadata = zk.getData(root, false, null);
        rootDataMonitor = new HashDataMonitor(zk, root, status, rootMetadata, nodeName, dataManager, adress);
        // set watcher to update hash ring upon callback
        rootMetadata = zk.getData(root, rootDataMonitor, status);
    }

    public String getRootMetadata() throws Exception {
        logger.info("Retrieveing root metadata to send to client ...");
        return rootDataMonitor.getMetadataString();
    }

    public void close() throws InterruptedException {
        connected = false;
        zk.close();
    }

}
