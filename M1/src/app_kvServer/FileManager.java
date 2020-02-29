package app_kvServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Semaphore;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class FileManager {

    private static Logger logger = Logger.getRootLogger();
    private static String dirPath = "/tmp/ECEdataSS/";
    private static String filePath = "/tmp/ECEdataSS/storage.txt";
    private static String valueSizeMapPath = "/tmp/ECEdataSS/vsmap.ser";
    private static String valueLocationMapPath = "/tmp/ECEdataSS/vlmap.ser";
    private Semaphore masterLock = new Semaphore(1);
    private Map<String, Integer> valueSizeMap = new HashMap<String, Integer>();
    private Map<String, Long> valueLocationMap = new HashMap<String, Long>();

    /**
     * Instantiate a DataManager object
     * @param logger KVServer will pass its logger
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *           to keep in-memory
     * @param strategy specifies the cache replacement strategy in case the cache
     *           is full and there is a GET- or PUT-request on a key that is
     *           currently not contained in the cache. Options are "FIFO", "LRU",
     *           and "LFU".
     */
    public FileManager() {
        try {
            readMaps();
            createStorage();
        } catch (Exception e) {
            logger.error("Error! Could not construct File Manager!", e);
        }
    }

    public boolean inStorage(String key) throws Exception {
        masterLock.acquire();
        boolean result = valueLocationMap.containsKey(key);
        masterLock.release();
        return result;
    }

    public String getKV(String key) throws Exception {
        masterLock.acquire();
        if (!valueLocationMap.containsKey(key)) {
            masterLock.release();
            return null;
        }
        try {
            long location = valueLocationMap.get(key);
            RandomAccessFile raf = new RandomAccessFile(filePath, "r");
            raf.seek(location);
            byte[] result = new byte[valueSizeMap.get(key)];
            raf.read(result);
            raf.close();
            return new String(result);
        } catch (Exception e) {
            return null;
        } finally {
            masterLock.release();
        }
    }

    public void putKV(String key, String value) throws Exception {
        masterLock.acquire();
        try {
            RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
            raf.seek(raf.length());
            valueLocationMap.put(key, raf.length());
            raf.write(value.getBytes());
            raf.close();
            valueSizeMap.put(key, value.getBytes().length);
            writeMaps();
        } finally {
            masterLock.release();
        }
    }

    public void delete(String key) throws Exception {
        masterLock.acquire();
        valueSizeMap.remove(key);
        valueLocationMap.remove(key);
        writeMaps();
        masterLock.release();
    }

    public void clearStorage() throws Exception {
        masterLock.acquire();
        try {
            File storageFolder = new File(dirPath);
            try{
                FileUtils.cleanDirectory(storageFolder);
            }catch(Exception e){
                storageFolder.mkdirs();
            }
            createStorage();
            valueSizeMap.clear();
            valueLocationMap.clear();
            writeMaps();
        } finally {
            masterLock.release();
        }
    }

    private void readMaps() throws Exception{
        File vsFile = new File (valueSizeMapPath);
        File vlFile = new File (valueLocationMapPath);
        if (vsFile.exists() && vlFile.exists()){
            FileInputStream vsStream = new FileInputStream(valueSizeMapPath);
            FileInputStream vlStream = new FileInputStream(valueLocationMapPath);
            ObjectInputStream vsOStream = new ObjectInputStream(vsStream);
            ObjectInputStream vlOStream = new ObjectInputStream(vlStream);
            this.valueSizeMap = (HashMap<String, Integer>) vsOStream.readObject();
            this.valueLocationMap = (HashMap<String, Long>) vlOStream.readObject();
            vsOStream.close();
            vlOStream.close();
            vsStream.close();
            vlStream.close();
        }
    }

    private void writeMaps() throws Exception{
        FileOutputStream vsStream = new FileOutputStream(valueSizeMapPath);
        FileOutputStream vlStream = new FileOutputStream(valueLocationMapPath);
        ObjectOutputStream vsOStream = new ObjectOutputStream(vsStream);
        ObjectOutputStream vlOStream = new ObjectOutputStream(vlStream);
        vsOStream.writeObject(this.valueSizeMap);
        vlOStream.writeObject(this.valueLocationMap);
        vsOStream.close();
        vlOStream.close();
        vsStream.close();
        vlStream.close();
    }

    private void createStorage() throws Exception{
        File storageFolder = new File(dirPath);
        if (!storageFolder.exists()){
            storageFolder.mkdirs();
        }
        File newStorage = new File(filePath);
        newStorage.createNewFile();
    }
}
