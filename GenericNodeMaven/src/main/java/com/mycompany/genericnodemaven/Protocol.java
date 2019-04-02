package com.mycompany.genericnodemaven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Protocol {
	private static final int PUT = 6;
	private static final int GET = 5;
	private static final int DEL = 5;
	private static final int STORE = 4;
	private static final String TCP = "tc";
	private static final int MAXRETRY = 10; // maximum retries to put / del
	private static final String CONFIGFILE = "./tmp/nodes.cfg";

	PrintWriter out = null;
	// create static map for key value store
	// private static final Map<String, String> map = new
	// ConcurrentHashMap<String, String>();

	private static final Map<String, String> MAP = Collections.synchronizedMap(new HashMap<String, String>());

	private static final ConcurrentMap<String, String> LOCKMAP = new ConcurrentHashMap<>();

	public String processInput(String theInput) throws IOException, URISyntaxException {

		String theOutput = theInput;
		//out = theOut;
		if (theInput == null) {
			return theOutput;
		}
		theInput = theInput.replace("]", "");
		theInput = theInput.replace("[", "");
		theInput = theInput.replace(",", "");

		String[] args = theInput.split(" ");
		if (args.length >= STORE) {
			// client to server communication
			theOutput = clientToServer(args);
		} else {
			// server to server communication
			theOutput = internalServer(args);
		}

		return theOutput;
	}

	public String clientToServer(String[] args) throws IOException, URISyntaxException {
		String theOutput = "Error in clientToServer Protocol.java";
		String protocol = args[0];
		String operation = args[3];
		switch (operation) {
		case "putTest":
			if (args.length != PUT) {
				wrongNumberOfInput();
			} else {
				theOutput = dPut2(args[4], args[5]);
			}
			break;
		case "put":
			if (args.length != PUT) {
				wrongNumberOfInput();
			} else {
				Map<String, String> nodes = new HashMap<String, String>();
//				nodes.put("localhost", "1234");
//                                nodes.put("localhost", "1235");
                                //nodes.put("172.17.0.2", "1234");
				//nodes.put("172.17.0.3", "1234");
				// for each line in the config file, read it and add to nodes
				try (BufferedReader br = new BufferedReader(new FileReader(CONFIGFILE))) {
                                    String line;
                                    while ((line = br.readLine()) != null) {
                                        String[] temp = line.split(":");
                                        nodes.put(temp[0], temp[1]);
                                        //System.out.println("node(k/v) from cfg added in put: " + temp[0]+":" + temp[1]);
                                    }
				 }

				int retryCount = 0;
				while (retryCount < MAXRETRY) {
					int successCount = 0;

					// dPut1(args[4], args[5]);

					for (Map.Entry<String, String> node : nodes.entrySet()) {
						String ip = node.getKey();
						int port = Integer.parseInt(node.getValue());
						Socket internalSocket = new Socket(ip, port);
						// TODO: MULTI THREAD THIS; parrallel
						String results = connectToInternalNode(internalSocket, "dput1 " + args[4] + " " + args[5]);
						switch (results) {
						case "ABORT":
							break;
						case "PUT":
							successCount++;
							break;
						default:
							// something broke...
							System.out.println("something broke in dput1 switch case");
							break;
						}
                                                
                                                internalSocket.close();
					}

					// TODO: CHECK IF SELF IS IN .size()...

					// check if all succeeded, abort if not, dput2 if succeeded
					if (successCount == nodes.size()) {
						for (Map.Entry<String, String> node : nodes.entrySet()) {
							String ip = node.getKey();
							int port = Integer.parseInt(node.getValue());
							Socket internalSocket = new Socket(ip, port);

							// TODO: MULTI THREAD THIS; parrallel
							connectToInternalNode(internalSocket, "dput2 " + args[4] + " " + args[5]);
                                                        
                                                        internalSocket.close();
                                                }
						theOutput = dPut2(args[4], args[5]);
						return theOutput;
					} else {
						// abort all nodes
						for (Map.Entry<String, String> node : nodes.entrySet()) {
							String ip = node.getKey();
							int port = Integer.parseInt(node.getValue());
							Socket internalSocket = new Socket(ip, port);

							// TODO: MULTI THREAD THIS; parrallel
							theOutput = connectToInternalNode(internalSocket, "dputabort " + args[4] + " " + args[5]);
                                                        
                                                        internalSocket.close();
                                                }

						// increment once for each time all nodes abort
						retryCount++;

					}
				}
				theOutput = "put key= hit the max retry limit for node lock";
			}
			break;

		case "get":
			if (args.length != GET) {
				wrongNumberOfInput();
			} else {
				// get response
				String value;
				synchronized (MAP) {
					value = MAP.get(args[4]);
				}
				theOutput = "get key=" + args[4] + " get val=" + value;
			}
			break;
		case "del":
				Map<String, String> nodes = new HashMap<String, String>();
//				nodes.put("localhost", "1234");
//                                nodes.put("localhost", "1235");
                                //nodes.put("172.17.0.2", "1234");
				//nodes.put("172.17.0.3", "1234");
				// for each line in the config file, read it and add to nodes
				try (BufferedReader br = new BufferedReader(new FileReader(CONFIGFILE))) {
                                    String line;
                                    while ((line = br.readLine()) != null) {
                                        String[] temp = line.split(":");
                                        nodes.put(temp[0], temp[1]);
                                        //System.out.println("node(k/v) from cfg added in del: " + temp[0]+":" + temp[1]);
                                    }
				 }

				int retryCount = 0;
				while (retryCount < MAXRETRY) {
					int successCount = 0;

					for (Map.Entry<String, String> node : nodes.entrySet()) {
						String ip = node.getKey();
						int port = Integer.parseInt(node.getValue());
                                                System.out.println(port);
						Socket internalSocket = new Socket(ip, port);

						// TODO: MULTI THREAD THIS; parallel
						String results = connectToInternalNode(internalSocket, "ddel1 " + args[4]);
						switch (results) {
						case "ABORT":
							break;
						case "DEL":
							successCount++;
							break;
						default:
							// something broke...
							System.out.println("something broke in ddel1 switch case");
							break;
						}
                                                
                                                internalSocket.close();
					}

					// TODO: CHECK IF SELF IS IN .size()...

					// check if all succeeded, abort if not, ddel2 if succeeded
					if (successCount == nodes.size()) {
						for (Map.Entry<String, String> node : nodes.entrySet()) {
							String ip = node.getKey();
							int port = Integer.parseInt(node.getValue());
							Socket internalSocket = new Socket(ip, port);

							// TODO: MULTI THREAD THIS; parallel
							connectToInternalNode(internalSocket, "ddel2 " + args[4]);
						
                                                        internalSocket.close();
                                                
                                                }
						theOutput = dDel2(args[4]);
						return theOutput;
					} else {
						// abort all nodes
						for (Map.Entry<String, String> node : nodes.entrySet()) {
							String ip = node.getKey();
							int port = Integer.parseInt(node.getValue());
							Socket internalSocket = new Socket(ip, port);

							// TODO: MULTI THREAD THIS; parrallel
							theOutput = connectToInternalNode(internalSocket, "ddelabort " + args[4]);
						
                                                        internalSocket.close();
                                                
                                                }

						// increment once for each time all nodes abort
						retryCount++;

					}
				}
				theOutput = "del key= hit the max retry limit for node lock";
			
			break;

		case "store":
			if (args.length != STORE) {
				wrongNumberOfInput();
			} else {
				// store response
				// build string output for store
				final StringBuilder builder = new StringBuilder();
				synchronized (MAP) {
					MAP.forEach((k, v) -> builder.append(("key:" + k + ":value:" + v + ":")));
				}
				theOutput = builder.toString();
			}
			break;
		case "exit":
			if (protocol.equals(TCP)) {
				System.exit(1);
			} else {
				theOutput = "exit";
				break;
			}
		default:
			System.err.println("Usage: java GenericNode <operation> " + operation + " not a valid operation command.");
		}
		return theOutput;
	}

	public String internalServer(String[] args) {
		String theOutput = "Error in internalServer Protocol.java";
		String operation = args[0];

		switch (operation) {
		case "dput1":
			// lock
			theOutput = dPut1(args[1], args[2]);
			break;
		case "dput2":
			// put
			theOutput = dPut2(args[1], args[2]);
			break;
		case "dputabort":
			// unlock
			dPutAbort(args[1], args[2]);
			break;
		case "ddel1":
			// lock
			theOutput = dDel1(args[1]);
			break;
		case "ddel2":
			// del
			theOutput = dDel2(args[1]);
			break;
		case "ddelabort":
			// unlock
			dDelAbort(args[1]);
			break;
		default:
			System.err.println("Usage: java GenericNode <operation> " + operation + " not a valid operation command.");
		}
		return theOutput;
	}

	public void dPutAbort(String key, String value) {
            synchronized (LOCKMAP) {
                LOCKMAP.remove(key, value);
            }
	}

	/**
	 * checks if key value pair locked
	 * 
	 * @param key
	 * @param value
	 * @return String "PUT" or "ABORT"
	 */
	public String dPut1(String key, String value) {
            String v = "";
            synchronized (LOCKMAP) {
		v = LOCKMAP.putIfAbsent(key, value);
            }
		if (v == null) {
			// null when no previous value
                    return "PUT";
		} else {
                    return "ABORT";
		}
	}

	/**
	 * commits a put in MAP
	 * 
	 * @param key
	 * @param value
	 * @return String "PUT" or "ABORT"
	 */
	public String dPut2(String key, String value) {
		synchronized (MAP) {
		MAP.put(key, value);
                }
                synchronized (LOCKMAP) {
                    LOCKMAP.remove(key);
                }
		return "put key=" + key;
		
	}

	private String wrongNumberOfInput() {
		return "Your input has too few or too many variables";
	}

	/**
	 * checks if key value pair is locked
	 * 
	 * @param key
	 * @return String "DEL" or "ABORT"
	 */
	public String dDel1(String key) {
		// String value = LOCKMAP.get(key);
		String v = LOCKMAP.putIfAbsent(key, "");
		if (v == null) {
			// null when no previous value
			return "DEL";
		} else {
			return "ABORT";
		}
	}

	/**
	 * commits a delete in MAP
	 * 
	 * @param key
	 * @return String "DEL" or "ABORT"
	 */
	public String dDel2(String key) {
		synchronized (MAP) {
                    MAP.remove(key);
                }
                
                //String value = LOCKMAP.get(key);
               
                synchronized (LOCKMAP) {
                    LOCKMAP.remove(key);
                }
                    return "delete key=" + key;
		
	}

	// unlock key
	public void dDelAbort(String key) {
            synchronized (LOCKMAP) {
		LOCKMAP.remove(key);
            }
	}

	// handle put / del operations for multinode server
	public String connectToInternalNode(Socket socket, String fromMaster) {
		try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
			// BufferedReader stdIn =
			// new BufferedReader(new InputStreamReader(System.in));
			String fromSlave;
			// String fromMaster;

			// sends operation message to slave node
			out.println(fromMaster);

			while ((fromSlave = in.readLine()) != null) {

				// return fromSlave;

				// TODO:refactor?? This is done b/c readline is perpetually
				// read?
				if (fromSlave.equals("ABORT"))
					return "ABORT";

				if (fromSlave.equals("DEL"))
					return "DEL";

				if (fromSlave.equals("PUT"))
					return "PUT";
			}
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host " + socket);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Prot: Couldn't get I/O for the connection to " + socket);
			System.exit(1);
		}
		return "SOMETHING REALLY WENT WRONG IN connectToInternalNode";
	}

}
