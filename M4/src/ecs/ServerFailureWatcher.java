package ecs;

import app_kvECS.ECSClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collection;

public class ServerFailureWatcher implements Watcher {
    ECSClient ecsClient;
    String serverName;
    ECSServerWatcher serverWatcher;
    String zkRoot;
    ZooKeeper zk;
    public ServerFailureWatcher(String root, ECSClient ecs , String name, ECSServerWatcher watcher, ZooKeeper zoo){
        zkRoot=root;
        ecsClient=ecs;
        serverName=name;
        serverWatcher=watcher;
        zk=zoo;
    }
    @Override
    public void process(WatchedEvent event) {

        String path= zkRoot+"/"+serverName;
        try {
            Stat status= zk.exists(path, false);
            if(status==null){
                serverWatcher.aliveServers.remove(serverName);
                ECSNode ecs = ecsClient.activeServerNodes.stream()
                        .filter(aliveNode -> serverName.matches(aliveNode.getNodeName()))
                        .findFirst()
                        .orElse(null);
                if(ecs!=null) {
                    Collection<String> namesToRemove = new ArrayList<>();
                    namesToRemove.add(serverName);
                    ecsClient.removeNodes(namesToRemove);
                }
            }
        }catch (Exception e){
            System.out.println("could not connect to exists");

        }
    }
}
