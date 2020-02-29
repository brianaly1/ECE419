package app_kvServer;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import ecs.ECSHashRing;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import app_kvServer.DataManager;

public class HashDataMonitor implements Watcher {

    private static Logger logger = Logger.getRootLogger();
    private ZooKeeper zk;
    private String root;
    private Stat status;
    private String rootDataString;
    private DataManager dataManager;
    private String nodeName;
    private String adress;

    public HashDataMonitor(ZooKeeper zk, String root, Stat status, byte[] rootData, String nodeName, DataManager dataManager, String adress) throws Exception {
        this.zk = zk;
        this.root = root;
        this.status = status;
        this.nodeName = nodeName;
        this.dataManager = dataManager;
        this.rootDataString = new String(rootData);
        this.adress = adress;
        updateDataManager();
    }

    public void process(WatchedEvent event) {
        try {
            logger.info("HashDataMonitor updating hash ring...");

            byte[] rootData = zk.getData(root, this, status);
            rootDataString = new String(rootData);
            if (rootDataString != "") {
                updateDataManager();
                logger.info("Start to purge:"+nodeName);
                dataManager.purge(false);

                logger.info("Start to new replicate:"+nodeName);
                dataManager.serverToNewReplicas();
                logger.info("HashDataMonitor updated hash ring.");
            }
            else
                logger.info("HashDataMonitor recieved empty metadata and the hash ring not updated.");

        } catch (KeeperException | InterruptedException eki) {
            logger.info("Unable to update hash ring upon callback ...",eki);
        } catch (Exception e) {
            logger.info("Unable to update hash ring upon callback ...",e);
        }
    }

    public void updateDataManager(){
        dataManager.initialize(rootDataString, adress);
    }

    public String getMetadataString() {
        return this.rootDataString;
    }

}
