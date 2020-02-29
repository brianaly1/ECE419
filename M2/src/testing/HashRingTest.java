package testing;

import java.math.BigInteger;
import java.net.UnknownHostException;

import client.KVStore;

import ecs.ECSHashRing;
import junit.framework.TestCase;


public class HashRingTest extends TestCase {


    public void testHashRing() {
        ECSHashRing hashRing = new ECSHashRing();
        assertTrue(hashRing.cached_map.isEmpty());
    }

    // TODO (sherwins): test this.
    public void testHashRingWithInput() {
        ECSHashRing hashRing = new ECSHashRing();
        assertTrue(hashRing.cached_map.isEmpty());
    }


    public void testGetHash() {
        assertEquals(ECSHashRing.getHash("1"), "c4ca4238a0b923820dcc509a6f75849b");
        assertNotSame(ECSHashRing.getHash("2"), "c4ca4238a0b923820dcc509a6f75849b");
    }

    public void testAdd() {
        ECSHashRing hashRing = new ECSHashRing();
        hashRing.add("127.0.0.1", 5000);
        hashRing.add("127.0.0.2", 5001);

        assertTrue(hashRing.cached_map.containsKey("127.0.0.1:5000"));
        assertTrue(hashRing.cached_map.containsValue("73909f8c96a9d08e876411c0a212a1f4"));

        assertTrue(hashRing.cached_map.containsKey("127.0.0.2:5001"));
        assertTrue(hashRing.cached_map.containsValue("c4416ee41299e74de2779c44c74f5e9f"));
    }

    public void testRemove() {
        ECSHashRing hashRing = new ECSHashRing();
        hashRing.add("127.0.0.1", 5000);
        hashRing.add("127.0.0.2", 5001);

        hashRing.remove("dummy");
        hashRing.remove("127.0.0.2:5001");

        assertTrue(hashRing.cached_map.size() == 1);
        assertTrue(hashRing.cached_map.containsKey("127.0.0.1:5000"));
        assertTrue(hashRing.cached_map.containsValue("73909f8c96a9d08e876411c0a212a1f4"));
    }

    public void testGetNodeByKeyFromEmptyHashRing() {
        ECSHashRing hashRing = new ECSHashRing();
        assertNull(hashRing.getNodeByKey("127.0.0.1:5000"));
    }

    public void testGetNodeByKeyFromSingleEntryHashRing() {
        ECSHashRing hashRing = new ECSHashRing();

        // MD5 of Server1: 73909f8c96a9d08e876411c0a212a1f4
        hashRing.add("127.0.0.1", 5000);

        // MD5 of 'a': 0cc175b9c0f1b6a831c399e269772661
        assertEquals(hashRing.getNodeByKey("a"), "127.0.0.1:5000");

        // MD of 'w': f1290186a5d0b1ceab27f4e77c0c5d68
        assertEquals(hashRing.getNodeByKey("w"), "127.0.0.1:5000");
    }

    public void testGetNodeByKeyFromMultipleEntriesHashRing() {
        ECSHashRing hashRing = new ECSHashRing();

        // MD5 of Server1: 2a7d2d999f355883428faffebb19ab00z
        // MD5 of Server2: 73909f8c96a9d08e876411c0a212a1f4
        // MD5 of Server3: c4416ee41299e74de2779c44c74f5e9f
        hashRing.add("127.0.0.1", 12);
        hashRing.add("127.0.0.1", 5000);
        hashRing.add("127.0.0.2", 5001);

        // MD5 of 'a': 0cc175b9c0f1b6a831c399e269772661
        assertEquals(hashRing.getNodeByKey("a"), "127.0.0.1:12");

        // MD5 of '21': 3c59dc048e8850243be8079a5c74d079
        assertEquals(hashRing.getNodeByKey("21"), "127.0.0.1:5000");

        // MD5 of '20': 98f13708210194c475687be6106a3b84
        assertEquals(hashRing.getNodeByKey("20"), "127.0.0.2:5001");

        // MD of 'w': f1290186a5d0b1ceab27f4e77c0c5d68
        assertEquals(hashRing.getNodeByKey("w"), "127.0.0.1:12");
    }
}

