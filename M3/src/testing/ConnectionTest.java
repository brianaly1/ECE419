package testing;

import java.net.UnknownHostException;

import client.KVStore;

import ecs.ECSNode;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 50000);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}


	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}

	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testECSConnnectionTest() {
		Exception ex = null;
		ECSNode ecsNode = new ECSNode("server1", "localhost", 50000);

		try {
			ecsNode.connectToServer();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testECSUnknownHost() {
		Exception ex = null;
		ECSNode ecsNode = new ECSNode("server1", "unknown", 50000);

		try {
			ecsNode.connectToServer();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}


	public void testECSIllegalPort() {
		Exception ex = null;
		ECSNode ecsNode = new ECSNode("server1", "localhost", 123456789);

		try {
			ecsNode.connectToServer();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof IllegalArgumentException);
	}



}

