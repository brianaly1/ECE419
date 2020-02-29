package ecs;

import app_kvECS.ECSClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

public class ECSServerWatcher implements Watcher {
    ECSClient client;
    ZooKeeper zk;
    String zkRoot;
    List<String> aliveServers;
    public ECSServerWatcher(ECSClient ecs,ZooKeeper zoo,String root){
        client=ecs;
        zk=zoo;
        zkRoot=root;
        aliveServers=new ArrayList<>();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            List<String> children=zk.getChildren(zkRoot,this);
            for(String server:children){
                System.out.println("ECSSERVERWATCHER:"+server);
                String ecs = aliveServers.stream()
                        .filter(aliveNode -> server.matches(aliveNode))
                        .findFirst()
                        .orElse(null);
                if(ecs==null){
                    aliveServers.add(server);
                    String path= zkRoot+"/"+server;
                    ServerFailureWatcher newFailureWatcher = new ServerFailureWatcher(zkRoot,client,server,this,zk);
                    zk.exists(path,newFailureWatcher);
                }
            }
        }catch (Exception e){
            System.out.println("could not kill server");
        }
    }
}
