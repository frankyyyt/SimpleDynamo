package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static java.net.InetAddress.*;

/**
 * Created by fengguotian on 4/25/16.
 */
//extend thread      implements Runnable
public class Client implements Runnable {

    Context content;
    static final String COLUMN_NAME_KEY = "key";
    static final String TABLE_NAME = "messages";
    static final String COLUMN_NAME_VALUE = "value";
    static final int SERVER_PORT = 10000;
    private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CURI = Uri.parse("content://" + AUTHORITY);
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    public static String[] REMOTE_PORT = {"11108","11112","11116","11120", "11124"};

    String msgs;
    static Socket socket1  = null;
    static Socket socket2  = null;
    static Socket socket3  = null;
    static Socket socket4  = null;
    static Socket socket5  = null;

    public  Client(String msgs) {
        this.msgs = msgs;
    }

    PrintWriter       out1=null;
    PrintWriter       out2=null;
    PrintWriter       out3=null;
    PrintWriter       out4=null;
    PrintWriter       out5=null;

//    BufferedReader    in1;
//    BufferedReader    in2;
//    BufferedReader    in3;
//    BufferedReader    in4;
//    BufferedReader    in5;


//
//    OutputStream output1=null;
//    OutputStream output2=null;
//    OutputStream output3=null;
//    OutputStream output4=null;
//    OutputStream output5=null;


    @Override
    public void run() {
        if (SimpleDynamoProvider.sockettable.size() != 5) {
            try {


//                {InetAddress inteAddress = InetAddress.getByName(server);
//                    SocketAddress socketAddress = new InetSocketAddress(inteAddress, port);
//                    // create a socket
//                    Socket   socket = new Socket();
//                    // this method will block no more than timeout ms.
//                    int timeoutInMs = 10*1000;   // 10 seconds
//                    socket.connect(socketAddress, timeoutInMs);
//                }

                InetAddress serverAddress =InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
                   Socket socket1 = new Socket(serverAddress, 11108);
//                final int timeOut = (300);
//                socket1.connect(new InetSocketAddress(serverAddress, 11108));
                SimpleDynamoProvider.sockettable.put("11108", socket1);
                Socket socket2 = new Socket(serverAddress, 11112);
                SimpleDynamoProvider.sockettable.put("11112", socket2);
                Socket socket3 = new Socket(serverAddress, 11116);
                SimpleDynamoProvider.sockettable.put("11116", socket3);
                Socket socket4 = new Socket(serverAddress, 11120);
                SimpleDynamoProvider.sockettable.put("11120", socket4);

                Socket socket5 = new Socket(serverAddress, 11124);
                SimpleDynamoProvider.sockettable.put("11124", socket5);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ;
        }

            socket1 = SimpleDynamoProvider.sockettable.get("11108");
            socket2 = SimpleDynamoProvider.sockettable.get("11112");
            socket3 = SimpleDynamoProvider.sockettable.get("11116");
            socket4 = SimpleDynamoProvider.sockettable.get("11120");
            socket5 = SimpleDynamoProvider.sockettable.get("11124");
        if (msgs != null) {
            String[] rec = msgs.split("&&");
            try{
                out1 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())), true);
                out2 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream())), true);
                out3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())), true);
                out4 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket4.getOutputStream())), true);
                out5 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket5.getOutputStream())), true);

                //Create BufferedReader object for receiving messages from server.
//                in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
//                in2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
//                in3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
//                in4 = new BufferedReader(new InputStreamReader(socket4.getInputStream()));
//                in5 = new BufferedReader(new InputStreamReader(socket5.getInputStream()));
                Log.d(TAG, "In/Out created");

                System.out.println("clientthread send out!!!!!! : " + msgs);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (rec[0].equals("insert")) {

                // Create PrintWriter object for sending messages to server.
                out1.println(msgs);
                out2.println(msgs);
                out3.println(msgs);
                out4.println(msgs);
                out5.println(msgs);

                System.out.println("clientthread send out!!!!!! : " + msgs);
            }

            if (rec[0].equals("delete1")) {

                out1.println(msgs);
                out2.println(msgs);
                out3.println(msgs);
                out4.println(msgs);
                out5.println(msgs);
//
            }
            if (msgs.equals("*")) {

                out1.println(msgs);
                out2.println(msgs);
                out3.println(msgs);
                out4.println(msgs);
                out5.println(msgs);
//                System.out.println("clientthread send out!!!!!! : " + msgs);
            }

            if (rec[0].equals("recover")) {

                out1.println(msgs);
                out2.println(msgs);
                out3.println(msgs);
                out4.println(msgs);
                out5.println(msgs);
//                System.out.println("clientthread send out!!!!!! : " + msgs);
//
            }

            if (rec[0].equals("recdata") ) {
//			String msgs="recdata"+"&&"+port + "&&" +keyy+"&&"+backup.get(keyy)+"\n";
                String msgs="recdata"+"&&"+rec[1]+"&&"+rec[2]+"&&"+rec[3]+"\n";
                out1.println(msgs);
                out2.println(msgs);
                out3.println(msgs);
                out4.println(msgs);
                out5.println(msgs);

            }
        }


    }
}
