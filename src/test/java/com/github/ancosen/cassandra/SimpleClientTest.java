package com.github.ancosen.cassandra;

import org.junit.Test;

import com.github.oscerd.cassandra.SimpleClient;


public class SimpleClientTest {

	@Test
	public void testClient() {
		SimpleClient client = new SimpleClient();
		client.connect("127.0.0.1");
		client.getSession();
		client.createSchema();
		client.loadData();
		client.querySchema();
		client.closeSession();
		client.close();
	}
}
