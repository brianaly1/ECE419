package testing;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.CacheManager;
import app_kvServer.CacheFactory;
import app_kvServer.FileManager;
import cache_impl.FifoCache;
import cache_impl.LinkedHashMapCache;
import cache_impl.LruCache;
import cache_impl.LfuCache;
import ecs.ECSNode;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import shared.messages.KVMessage;
import shared.messages.ProtoKVMessage;

public class AdditionalTest extends TestCase {

	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}

    // Test protobuf-KVMessage implementation.
	public void testProtoKVMessageGetSet() throws Exception{
		KVMessage message = new ProtoKVMessage("test_key", "test_value", KVMessage.StatusType.PUT_UPDATE);
        assertEquals(message.getKey(), "test_key");
        assertEquals(message.getValue(), "test_value");
        assertEquals(message.getStatus(), KVMessage.StatusType.PUT_UPDATE);
	}

    public void testProtoKVMessageInOutStream() throws Exception {
        ProtoKVMessage message = new ProtoKVMessage("test_key", "test_value", KVMessage.StatusType.PUT_UPDATE);
        ProtoKVMessage messageReceived = new ProtoKVMessage();

        OutputStream out = new ByteArrayOutputStream();
        message.writeMessage(out);
        byte[] byteArray = ((ByteArrayOutputStream) out).toByteArray();

        try {
            InputStream in = new ByteArrayInputStream(byteArray);
            messageReceived.parseMessage(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(message.getKey(), messageReceived.getKey());
        assertEquals(message.getValue(), messageReceived.getValue());
        assertEquals(message.getStatus(), messageReceived.getStatus());
    }

    // Test File Manager implementation.
    public void testServerFilePutGetAndInStorage() {
        FileManager fileManager = new FileManager();
        String result1 = "";
        String result2 = "";
        boolean inStorage1 = false;
        boolean inStorage2 = true;

        try {
            fileManager.putKV("key1", "value1");
            fileManager.putKV("key2", "value2");
            result1 = fileManager.getKV("key1");
            result2 = fileManager.getKV("key2");
            inStorage1 = fileManager.inStorage("key1");
            inStorage2 = fileManager.inStorage("key3");
         } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(result1, "value1");
        assertEquals(result2, "value2");
        assertEquals(inStorage1, true);
        assertEquals(inStorage2, false);
    }

    public void testServerFileDelete() {
        FileManager fileManager = new FileManager();
        boolean inStorage1 = false;
        boolean inStorage2 = true;

        try {
            fileManager.putKV("key1", "value1");
            inStorage1 = fileManager.inStorage("key1");
            fileManager.delete("key1");
            inStorage2 = fileManager.inStorage("key1");
         } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(inStorage1, true);
        assertEquals(inStorage2, false);
    } 

    public void testServerFileClear() {
        FileManager fileManager = new FileManager();
        boolean inStorage = false;
        boolean inStorage1 = true;
        boolean inStorage2 = true;

        try {
            fileManager.putKV("key1", "value1");
            fileManager.putKV("key2", "value2");
            inStorage = fileManager.inStorage("key1");
            fileManager.clearStorage();
            inStorage1 = fileManager.inStorage("key1");
            inStorage2 = fileManager.inStorage("key2");
         } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(inStorage, true);
        assertEquals(inStorage1, false);
        assertEquals(inStorage2, false);
    }

    // Test Cache Manager implementation.
    public void testServerCachePutAndGet() {
        try {
            for (CacheStrategy strategy : CacheStrategy.values()) {
                CacheManager cacheManager = new CacheFactory().GetCache(10, strategy);
                cacheManager.putKV("key1", "value1");
                assertEquals(cacheManager.getKV("key1"), "value1");
            }
        } catch (Exception e) {
        }
    }

    public void testFifoCache() {
        CacheManager cacheManager = new CacheFactory().GetCache(3, CacheStrategy.FIFO);
	    try {
            cacheManager.putKV("key1", "value1");
            cacheManager.putKV("key2", "value2");
            cacheManager.putKV("key3", "value3");
            cacheManager.putKV("key4", "value4");
            cacheManager.putKV("key5", "value5");
        } catch (Exception e) {
	        e.printStackTrace();
        }

        Set<String> keys = ((FifoCache) cacheManager).map.keySet();
        assertEquals("[key3, key4, key5]", keys.toString());

        try {
            cacheManager.getKV("key3");
            cacheManager.putKV("key1", "value1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals("[key4, key5, key1]", keys.toString());
    }

    public void testLruCache() {
        CacheManager cacheManager = new CacheFactory().GetCache(3, CacheStrategy.LRU);
        try {
            cacheManager.putKV("key1", "value1");
            cacheManager.putKV("key2", "value2");
            cacheManager.putKV("key3", "value3");
            cacheManager.putKV("key4", "value4");
            cacheManager.putKV("key5", "value5");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Set<String> keys = ((LruCache) cacheManager).map.keySet();
        assertEquals("[key3, key4, key5]", keys.toString());

        try {
            cacheManager.getKV("key3");
            cacheManager.putKV("key1", "value1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals("[key5, key3, key1]", keys.toString());
    }

    public void testLfuCache() {
        CacheManager cacheManager = new CacheFactory().GetCache(3, CacheStrategy.LFU);
        try {
            cacheManager.putKV("key1", "value1");
            cacheManager.putKV("key2", "value2");
            cacheManager.getKV("key2");
            cacheManager.getKV("key2");
            cacheManager.putKV("key3", "value3");
            cacheManager.getKV("key3");
            cacheManager.getKV("key3");
            cacheManager.getKV("key3");
            cacheManager.putKV("key4", "value4");
            cacheManager.putKV("key5", "value5");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Set<String> keys1 = ((LfuCache) cacheManager).mapValues.keySet();
        Set<String> expected1 = new HashSet<String>(Arrays.asList("key2", "key3", "key5"));
        assertTrue(expected1.containsAll(keys1));

        try {
            cacheManager.getKV("key5");
            cacheManager.getKV("key5");
            cacheManager.getKV("key5");
            cacheManager.putKV("key6", "value6");
            cacheManager.getKV("key6");
            cacheManager.getKV("key6");
            cacheManager.getKV("key6");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Set<String> keys2 = ((LfuCache) cacheManager).mapValues.keySet();
        Set<String> expected2 = new HashSet<String>(Arrays.asList("key6", "key3", "key5"));
        assertTrue(expected2.containsAll(keys2));
    }

    public void testECSGetNodeMetaData() {
        ECSNode ecsNode = new ECSNode("server1", "localhost", 50000);
        assertTrue(Arrays.equals(ecsNode.getNodeMetaData(), (new String("server1,localhost,50000")).getBytes()));

    }

}
