package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
		KVClient app = new KVClient();
    	return app;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
		KVServer app = new KVServer(port, cacheSize, strategy);
		return app;
	}
}