package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/* any necessary Java packages here */

public class RUStoreClient {

	/* any necessary class members here */
	private String host;
	private int port;
	private Socket clientSocket;
	
	
	private DataOutputStream toServer;
    private DataInputStream fromServer;

	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here
		this.host=host;
		this.port=port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public void connect() throws UnknownHostException, IOException {

		// Implement here
		clientSocket=new Socket(host,port);
		toServer = new DataOutputStream(clientSocket.getOutputStream());
        fromServer = new DataInputStream(clientSocket.getInputStream());
		
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	
	public int put(String key, byte[] data) throws IOException {

		
		
		byte[] action="put".getBytes();
		byte[] keyByte =key.getBytes();
		byte[] msg =data;
		
		toServer.writeInt(action.length);
		toServer.write(action);
		
		toServer.writeInt(keyByte.length);
		toServer.write(keyByte);
		
		toServer.writeInt(msg.length);
		toServer.write(msg);

		
		int ret=fromServer.readInt();
		
		return ret;


	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, String file_path) throws IOException {

		
//		File file=new File(file_path);
//		
//		if(!file.exists()) {
//			System.out.println("FIle not found");
//			return -1;
//		}
//		
//		byte[] data=new byte[(int) file.length()];
//		
//		BufferedInputStream strm=new BufferedInputStream(new FileInputStream(file));
//		
//		int i=0;
//		int j=0;
//		
//		while(j<data.length) {
//			i=strm.read(data,j,(data.length-j));
//			j+=i;
//		}
//		
//		byte[] action="put".getBytes();
//		byte[] keyByte =key.getBytes();
//		
//		toServer.writeInt(action.length);
//		toServer.write(action);
//		
//		toServer.writeInt(keyByte.length);
//		toServer.write(keyByte);
//		
//		toServer.writeInt(data.length);
//		toServer.write(data);
//		
		
		
		File f=new File(file_path);
		
		
		Path path=Paths.get(file_path);

		
		if(f.exists()&&!f.isDirectory()) {
			path=Paths.get(file_path);
			byte[] msg=Files.readAllBytes(path);
			


			byte[] action="put".getBytes();
			byte[] keyByte =key.getBytes();
			
			toServer.writeInt(action.length);
			toServer.write(action);
			
			toServer.writeInt(keyByte.length);
			toServer.write(keyByte);
			
			toServer.writeInt(msg.length);
			toServer.write(msg);
			
			int ret=fromServer.readInt();
			
			return ret;
		}
		

		

		return -1;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public byte[] get(String key) throws IOException {

		byte[] action="get".getBytes();
		byte[] keyByte =key.getBytes();
		
		toServer.writeInt(action.length);
		toServer.write(action);
		
		toServer.writeInt(keyByte.length);
		toServer.write(keyByte);
		
		int dataLen=fromServer.readInt();
		
		if(dataLen==-1) {
			return null;
		}
		
		byte[] data=new byte[dataLen];
		fromServer.readFully(data,0,data.length);
		
		
		// Implement here
		return data;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int get(String key, String file_path) throws IOException {

		byte[] action="get".getBytes();
		byte[] keyByte =key.getBytes();
		
		toServer.writeInt(action.length);
		toServer.write(action);
		
		toServer.writeInt(keyByte.length);
		toServer.write(keyByte);
		
		int dataLen=fromServer.readInt();
		
		if(dataLen==-1) {
			return 1;
		}
		
		byte[] data=new byte[dataLen];
		fromServer.readFully(data,0,data.length);
		
		try (FileOutputStream fos = new FileOutputStream(file_path)) {
			   fos.write(data);
			   //fos.close(); There is no more need for this line since you had created the instance of "fos" inside the try. And this will automatically close the OutputStream
			}
		
		
		// Implement here
		return 0;

	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int remove(String key) throws IOException {

		byte[] action="remove".getBytes();
		byte[] keyByte =key.getBytes();
		
		toServer.writeInt(action.length);
		toServer.write(action);
		
		toServer.writeInt(keyByte.length);
		toServer.write(keyByte);
		
		int ret=fromServer.readInt();
		// Implement here
		return ret;

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public String[] list() throws IOException {
		List<String> list=new ArrayList<>();
		
		byte[] action="list".getBytes();
		toServer.writeInt(action.length);
		toServer.write(action);
		
		int keyCt=fromServer.readInt();
		
		for(int i=0;i<keyCt;i++) {
			int dataLen=fromServer.readInt();
			byte[] data=new byte[dataLen];
			fromServer.readFully(data,0,data.length);
			
			list.add(new String(data));
		}
		
		
		String[] finList=new String[list.size()];
		list.toArray(finList);
		
		
		if(finList.length>0) {
			return finList;
		}
		else {
			return null;
		}

	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {

		
		
		
		byte[] action="disconnect".getBytes();
		
		toServer.writeInt(action.length);
		toServer.write(action);
		


	}

}
