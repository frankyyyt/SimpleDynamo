package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Created by fengguotian on 4/26/16.
 */
public class server implements Runnable {

    //initialize the stature prepare for failure detection

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CURI = Uri.parse("content://" + AUTHORITY);

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Socket socket = serverSocket.accept();
                new Thread(new receive(socket)).start();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}



