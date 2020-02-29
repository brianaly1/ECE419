package testing;

import java.net.UnknownHostException;

import app_kvClient.KVClient;
import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.util.ArrayList;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	

	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void testTwoClients() {

		String key = "testTwo";
		String value= "unitTest";
		String valueUpdated= "unitTestUpdated";
		KVMessage response=null;
		KVMessage response1=null;
		KVMessage response2=null;
		KVMessage response3=null;
		KVMessage response4=null;
		KVMessage response5=null;
		Exception ex = null;
		KVStore client2 = new KVStore("localhost", 50000);
		try {
			client2.connect();
		} catch (Exception e) {
			ex=e;
		}

		try {
			response = kvClient.put(key,value);
			response1 = client2.get(key);
			response2 = client2.put(key,valueUpdated);
			response3 = kvClient.get(key);
			response4 = client2.put(key,"");
			response5 = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		try {
			client2.disconnect();
		} catch (Exception e) {
			ex=e;
		}

		assertTrue(ex == null
				&& response.getStatus() == StatusType.PUT_SUCCESS
				&& response1.getStatus() == StatusType.GET_SUCCESS
				&& response2.getStatus() == StatusType.PUT_UPDATE
				&& response3.getStatus() == StatusType.GET_SUCCESS
				&& response4.getStatus() == StatusType.DELETE_SUCCESS
				&& response5.getStatus() == StatusType.GET_ERROR
		);

	}

	public void testECSConnectTest() {
		Exception ex = null;
		ECSNode ecsNode = new ECSNode("server1", "localhost", 50000);

		try {
			ecsNode.connectToServer();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testECSConnectUnknownHost() {
		Exception ex = null;
		ECSNode ecsNode = new ECSNode("server1", "unknown", 50000);

		try {
			ecsNode.connectToServer();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}


	public void testECSConnectIllegalPort() {
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
