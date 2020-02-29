package ecs;

import app_kvServer.HashDataMonitor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;
import java.util.HashSet;
import java.util.Set;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class ECSHashRing {

    static final int NUM_REPLICATION = 2;

    // Maps from addressAndPort to the hashed value.
    public Map<String, String> cached_map;
    private boolean initialized = false;

    public ECSHashRing() {
        cached_map = new HashMap<String, String>();
    }

    public ECSHashRing(String encodedHashRing) {
        cached_map = new HashMap<String, String>();
        try {
            Map<String, String> temp = deserializeMap(encodedHashRing);
            for (Map.Entry<String, String> entry : temp.entrySet()) {
                cached_map.put(entry.getValue(), getHash(entry.getValue()));
                System.out.println("Map Entry:"+entry.getKey()+","+entry.getValue());
            }
            initialized = true;
        } catch (Exception e){
            System.out.println("Something went wrong in ECSHashRing deserializeMap.");
            System.out.println("Check the format again.");
        }
    }

    public static String getHash(String str) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            md.reset();
        } catch (Exception e) {

        }
        md.update(str.getBytes());
        byte[] digest = md.digest();
        BigInteger bigInt = new BigInteger(1,digest);
        String hashtext = bigInt.toString(16);
        return hashtext;
    }

    public void add(String fullAddress) {
        cached_map.put(fullAddress, getHash(fullAddress));
    }

    public void add(String address, int port) {
        String fullAddress = address + ":" + Integer.toString(port);
        this.add(fullAddress);
    }

    public void remove(String addressAndPort) {
        if (cached_map.containsKey(addressAndPort))
            cached_map.remove(addressAndPort);
        else {
            System.out.println("Something went wrong, could not find the key in cached hash ring.");
        }
    }

    // Gets the coordinator address and port by the hashed value
    public String getNodeByHash(String hash) {
        if (cached_map.isEmpty()) {
            System.out.println("Cached hash ring is empty.");
            return null;
        }
        // If there is only one cached server address, return it as there is only one server responsible for.
        if (cached_map.size() == 1) {
            return (String) cached_map.keySet().toArray()[0];
        }

        String foundFullAddress = null;

        // Try to find one bigger or equal to the hash.
        BigInteger currentKey = new BigInteger(hash, 16);
        BigInteger oneBiggerOrEqualFound = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
        for (Map.Entry<String, String> entry : cached_map.entrySet()) {
            BigInteger currentEntry = new BigInteger(entry.getValue(), 16);
            if ((currentEntry.compareTo(currentKey) > 0 || currentEntry.equals(currentKey))
                    && currentEntry.compareTo(oneBiggerOrEqualFound) < 0) {
                oneBiggerOrEqualFound = currentEntry;
                foundFullAddress = entry.getKey();
            }
        }

        if (foundFullAddress != null)
            return foundFullAddress;

        // If not found find the smallest one.
        BigInteger min = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
        for (Map.Entry<String, String> entry : cached_map.entrySet()) {
            BigInteger currentEntry = new BigInteger(entry.getValue(), 16);
            if (min.compareTo(currentEntry) > 0) {
                min = currentEntry;
                foundFullAddress = entry.getKey();
            }
        }
        return foundFullAddress;
    }

    // Gets addressAndPort of the coordinator of the input key.
    public String getNodeByKey(String key) {
        return getNodeByHash(getHash(key));
    }

    // Gets a list of addressAndPorts of a given a hash.
    public List<String> GetReplicasByHash(String hash) {
        // Check if there is no replication.
        if (cached_map.size() <= 1) return null;

        List<String> replicas_full_addresses = new LinkedList<>();
        String coordinator_full_address = getNodeByHash(hash);

        // If cached_map is less than the total number of replication, return all cached_map except the
        // coordinator.
        if (cached_map.size() <= NUM_REPLICATION + 1) {  // Add +1 to encounter the coordinator as well.
            replicas_full_addresses = new LinkedList<>(cached_map.keySet());
            replicas_full_addresses.remove(coordinator_full_address);
            return replicas_full_addresses;
        }

        // Find the replicas by iterating over the ring.
        String iter_string = coordinator_full_address;
        for (int i = 0; i < NUM_REPLICATION; ++i) {
            iter_string = getNextNode(getHash(iter_string));
            replicas_full_addresses.add(iter_string);
        }
        return replicas_full_addresses;
    }

    // Gets a list of addressAndPorts of a given a key.
    public List<String> GetReplicasByKey(String key) {
        return GetReplicasByHash(getHash(key));
    }
    
    // Gets the coordinators given a replicas serverAddress.
    public HashSet<String> getCoordinators(String serverAddress) {
        // Check if there is no replication.
        if (cached_map.size() <= 1) return null;

        HashSet<String> coordinators = new HashSet<>();

        if (cached_map.size() <= NUM_REPLICATION + 1) {
            coordinators = new HashSet<>(cached_map.keySet());
            coordinators.remove(serverAddress);
            return coordinators;
        }

        // Find the coordinators by iterating over the ring.
        String iter_string = serverAddress;
        for (String node : cached_map.keySet()) {
            if (GetReplicasByKey(node).contains(serverAddress))
                coordinators.add(node);
        }

        return coordinators;
    }

    // Gets the next hashed value in the hash ring given a hashed value.
    public String getNextNode(String hashedValue) {
        BigInteger nextHash = (new BigInteger(hashedValue, 16)).add(BigInteger.ONE);
        return getNodeByHash(nextHash.toString(16));
    }

    public static Map<String,String> deserializeMap(String s) throws Exception {
        Map<String,String> dataMap;
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        dataMap = (HashMap<String, String>) ois.readObject();
        ois.close();
        return dataMap;
    }

    public boolean inRing(String serverAdress) {
        if (!initialized) {
            return false;
        }
        return cached_map.containsKey(serverAdress);
    }

    public boolean isUnity() {
        if (!initialized) {
            return true;
        }
        return cached_map.size() == 1;
    }

}
