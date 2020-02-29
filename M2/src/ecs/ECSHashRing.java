package ecs;

import app_kvServer.HashDataMonitor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ECSHashRing {
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
        cached_map.put(fullAddress, getHash(fullAddress));
    }

    public void remove(String addressAndPort) {
        if (cached_map.containsKey(addressAndPort))
            cached_map.remove(addressAndPort);
        else {
            System.out.println("Something went wrong, could not find the key in cached hash ring.");
        }
    }

    public String getNodeByKey(String key) {
        System.out.println("GetNodeByKey:"+key);
        if (cached_map.isEmpty()) {
            System.out.println("Cached hash ring is empty.");
            return null;
        }
        if (cached_map.size() == 1) {
            return (String) cached_map.keySet().toArray()[0];
        }

        String foundFullAddress = null;

        // Try to find one bigger one.
        BigInteger currentKey = new BigInteger(getHash(key), 16);
        BigInteger oneBiggerFound = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
        for (Map.Entry<String, String> entry : cached_map.entrySet()) {
            BigInteger currentEntry = new BigInteger(entry.getValue(), 16);
            if (currentEntry.compareTo(currentKey) > 0
                    && currentEntry.compareTo(oneBiggerFound) < 0) {
                oneBiggerFound = currentEntry;
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
