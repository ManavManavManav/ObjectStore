package com.RUStore;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/* any necessary Java packages here */

public class RUStoreServer {
	
	

	/* any necessary class members here */

	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException{

		HashMap<String, byte[]> storage=new HashMap<String, byte[]>();
		
		
		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);


		// Implement here //
		
		ServerSocket serverSocket=new ServerSocket(port);
		Socket clientSocket=serverSocket.accept();
		
		
		
		DataInputStream fromClient = new DataInputStream(clientSocket.getInputStream());
		DataOutputStream toClient = new DataOutputStream(clientSocket.getOutputStream());

		
		
		
		while(true) {
			int actionLen=fromClient.readInt();
			byte[] byteAction=new byte[actionLen];
			fromClient.readFully(byteAction,0,byteAction.length);
			String command=new String(byteAction);
			
			
			if(command.equals("disconnect")) {
				System.out.println("disconnect received");
				break;
			}
			else if(command.equals("list")) {
				String[] listOfKeys=storage.keySet().toArray(new String[0]);
				toClient.writeInt(listOfKeys.length);
				
				for(String s:listOfKeys) {
					toClient.writeInt(s.length());
					toClient.write(s.getBytes());
				}
				
				continue;
			}
			
			int keyLen=fromClient.readInt();
			byte[] byteKey=new byte[keyLen];
			fromClient.readFully(byteKey,0,byteKey.length);
			
			
			String key=new String(byteKey);
			
			
			if(command.equals("remove")) {
				if(storage.containsKey(key)) {
					storage.remove(key);
					toClient.writeInt(0);
				}
				else {
					toClient.writeInt(1);
				}
			}
			
			
			if(command.equals("put")) {
				int msgLen=fromClient.readInt();
				byte[] msg=new byte[msgLen];
				fromClient.readFully(msg,0,msg.length);
				
				if(storage.containsKey(key)) {
					toClient.writeInt(1);
				}
				else {
					storage.put(key,msg);
					toClient.writeInt(0);
				}
				
				
			}
			
			if(command.equals("get")) {
				if(storage.containsKey(key)) {
					toClient.writeInt(storage.get(key).length);
					toClient.write(storage.get(key));
					System.out.println(new String(storage.get(key))+"--retrieved item");
				}
				else {
					toClient.writeInt(-1);
				}
			}

		}
		
		System.out.println("Client DCED");
		
		for(String a:storage.keySet()) {
			
			System.out.println(a+"--fromKeySet");
		}
		
		
        clientSocket.close();
		


	}

}
