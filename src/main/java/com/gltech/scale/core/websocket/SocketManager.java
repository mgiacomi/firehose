package com.gltech.scale.core.websocket;

import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;

public class SocketManager
{
	public static void main(String[] args) {
		String destUri = "ws://echo.websocket.org";
		if (args.length > 0) {
			destUri = args[0];
		}
		WebSocketClient client = new WebSocketClient();
//		AggregatorSocket socket = new AggregatorSocket(callback, socketIO, modelIO);
		try {
			client.start();
			URI echoUri = new URI(destUri);
			ClientUpgradeRequest request = new ClientUpgradeRequest();
//			client.connect(socket, echoUri, request);
			System.out.printf("Connecting to : %s%n", echoUri);
			//socket.awaitClose(5, TimeUnit.SECONDS);
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			try {
				client.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
