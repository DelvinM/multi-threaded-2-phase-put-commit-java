/*
 * Copyright (c) 1995, 2014, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 
package com.mycompany.genericnodemaven;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author delvin mackenzie
 */
public class GenericNode {
    
    private static final int INTERNALPORT = 1234;
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        
        //add rmi? ts/us
        //TODO: REFACTOR
    	//System.out.println(Arrays.toString(args));
        if (args.length > 0 && ("dput1".equals(args[0])||"dput2".equals(args[0])||"dputabort".equals(args[0])||
                "ddel1".equals(args[0])||"ddel2".equals(args[0])||"ddelabort".equals(args[0])))
        {
        //	System.out.println("Before inter");
            //case when node to node communication
            interServerCommunication(args);
            
        } else if (args.length == 2) {
            // server input
            serverCode(args);
        } else if (args.length == 4 ||  args.length == 5 || args.length == 6) {
            // client input
            if(args[0].equals("tc")) {
                tcpClientCode(args);
            } else if (args[0].equals("uc")){
                udpClientCode(args);
            } else {
                System.err.println("incorrect protocol type");
            }
        } else {
            // catch all for illegal input + empty input with help message
            helpMessage();
        }
    }
    
    // method to communicate between servers
    private static void interServerCommunication(String[] args) throws IOException{
        
        //TODO: READ PORT FROM FILE?? or 1234 good?
        int portNumber = INTERNALPORT;
        boolean listening = true;
        //System.out.println("IN INTERSERVER");
        try (ServerSocket serverSocket = new ServerSocket(portNumber)){ 
            while (listening) {
                //starts multi threading code
            //	System.out.println("PORT: " + portNumber);
            //	System.out.println("IP" + serverSocket.getInetAddress());
                new MultiServerThread(serverSocket.accept()).start();
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + portNumber);
            System.exit(-1);
        }    
    }
    
    //runs server side, args is array of user input
    private static void serverCode (String[] args) throws IOException {
        
        String protocol = args[0];
        int portNumber = Integer.parseInt(args[1]);
        //System.out.println("Made it to serverCode!");    
        //start tcp server
        if (protocol.equals("ts")) {
            boolean listening = true;

            try (ServerSocket serverSocket = new ServerSocket(portNumber)) { 
//            	System.out.println("Socket IP: "+ serverSocket.getInetAddress());  
//            	System.out.println("Socket Port: "+ serverSocket.getLocalPort()); 
//            	System.out.println("Socket LocalSocketAdd: "+ serverSocket.getLocalSocketAddress());  
                while (listening) {
                	
                    //starts multi threading code
                    new MultiServerThread(serverSocket.accept()).start();
                }
            } catch (IOException e) {
                System.err.println("Could not listen on port " + portNumber);
                System.exit(-1);
            }
        } else if (protocol.equals("us")) { 
            //start udp server
            new UdpServerThread(portNumber).start();
            
        } else {
            System.err.println("incorrect protocol type");
            System.exit(-1);
        }   
    }
    
    private static void helpMessage () {
        //print out available inputs for GenericNode.jar
        System.out.println("Usage:");
        System.out.println("Client:");
        System.out.println("uc/tc <address> <port> put <key> <msg>  "
                + "UDP/TCP CLIENT: Put an object into store");
        System.out.println("uc/tc <address> <port> get <key>  "
                + "UDP/TCP CLIENT: Get an object from store by key");
        System.out.println("uc/tc <address> <port> del <key>  UDP/TCP CLIENT: "
                + "Delete an object from store by key");
        System.out.println("uc/tc <address> <port> store  UDP/TCP CLIENT:"
                + " Display object store");
        System.out.println("uc/tc <address> <port> exit  UDP/TCP CLIENT: "
                + "Shutdown server");

        System.out.println("Server:");
        System.out.println("us/ts <port>  UDP/TCP/TCP-and-UDP SERVER:"
                + " run server on <port>.");
    }
    
    private static void udpClientCode (String[] args) throws SocketException, UnknownHostException, IOException {
        
            // get a datagram socket
        DatagramSocket socket = new DatagramSocket();
        String hostName = args[1];
        int portNumber = Integer.parseInt(args[2]);
        String operation = args[3];
        
            //convert user input to string then bytes
        String fromUser = Arrays.toString(args);
        byte[] buf = fromUser.getBytes();
        
            // send request
        InetAddress address = InetAddress.getByName(hostName);
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, portNumber);
        socket.send(packet);
       
        buf = new byte [65001]; //reset buf for potential big incoming message
            // get response
        packet = new DatagramPacket(buf, buf.length); 
        socket.receive(packet);
        
            // display response
        String received = new String(packet.getData(), 0, packet.getLength());
        
        //notify user the output is trimmed
        if (packet.getLength() > 65000) {
             System.out.println("TRIMMED:\n");
        }
        
        //print server response
        printServerResponse(operation, received);
     
        socket.close();
    }
    
    //runs client side, args is array of user input
    private static void tcpClientCode (String[] args) {
        
        String hostName = args[1];
        int portNumber = Integer.parseInt(args[2]);
        String operation = args[3];
        
        try (
            Socket socket = new Socket(hostName, portNumber);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        ) {
            String fromServer;
            String fromUser;
            
          //  System.out.println(Arrays.toString(args));
            //loops until server response with results, then closes socket
            while ((fromServer = in.readLine()) != null) {
                
                if (args != null) {
                    //turn array args into string
                    fromUser = Arrays.toString(args);
                    fromUser = fromUser.substring(1, fromUser.length()-1).replace(",", "");
                    out.println(fromUser);
                    args = null;
                }
                
                if (!fromServer.equals("null")) {
                    //print server response
                    printServerResponse(operation, fromServer);
                }
            }
            in.close();
            out.close();
            socket.close();
            
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
                hostName);
            System.exit(1);
        }
          
    }
    
    private static void printServerResponse (String operation, String received) {
        //delimeter store message by line
        if (operation.equals("store") && !received.equals("exit")) {
            //new line for every 4 colon
            Pattern p = Pattern.compile("(.*?:.*?:.*?:.*?:)");
            Matcher m = p.matcher(received);
            while (m.find())
                System.out.println(m.group(0));
            
        } else if (!received.equals("exit")) { 
                //normal case that isn't exit
            System.out.println("server response:" + received);
        }
    }
    
}
