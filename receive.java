package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by fengguotian on 5/4/16.
 */
public class receive implements Runnable {

    //initialize the stature prepare for failure detection

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String COLUMN_NAME_KEY = "key";
    static final String TABLE_NAME = "messages";
    static final String COLUMN_NAME_VALUE = "value";

    int numsuc=0;
    int numpre=0;

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    //    authorities="edu.buffalo.cse.cse486586.simpledynamo.provider"
    private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CURI = Uri.parse("content://" + AUTHORITY);
    Socket sockets;
    public  receive(Socket socket) {
        this.sockets = socket;
    }

    @Override
    public void run() {

        BufferedReader in = null;
        try {

            in = new BufferedReader(new InputStreamReader(sockets.getInputStream()));
            System.out.println("Serverthread start!!!!!! ");

        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                String strings = in.readLine();
//                System.out.println("server thread start receive!!!!!! : " + strings);
                if (strings!=null ) {
                    String[] rec= strings.split("&&");
                    if (rec[0].equals("insert")) {
                        ContentValues cv = new ContentValues();
                        String key="insert" + "&&" + rec[1];
                        cv.put(COLUMN_NAME_KEY, key);
                        cv.put(COLUMN_NAME_VALUE, rec[2]);
                        SimpleDynamoProvider.put(cv);
                        System.out.println("Serverthread !!!!!! " + strings);
                    }
                    if (rec[0].equals("delete1")) {
                        SimpleDynamoProvider.deletestore(rec[1]);
                    }
                    if (strings.equals("*")) {
                        SimpleDynamoProvider.deletestore(strings);
                    }
                    if (rec[0].equals("recover")) {

                        String port=SimpleDynamoProvider.portstr;
                        if(SimpleDynamoProvider.getSuccessor1(port).equals(rec[1])){
                            numsuc++;

                        }
                        if(SimpleDynamoProvider.getpre1(port ).equals(rec[1])){
                            numpre++;
                       }

                        System.out.println("receiver recover request !!port  " + rec[1]+"num"+numpre+" : "+numsuc);

                    }

                    if((numpre>=3 )){
                        SimpleDynamoProvider.recover(rec[1]);
                        System.out.println(" recover port  " + rec[1] );
                    }

                    if((numsuc>=3 )){
                        SimpleDynamoProvider.recover(rec[1]);
                        System.out.println(" recover port  " + rec[1] );
                    }

                    if (rec[0].equals("recdata")) {
                        String port=SimpleDynamoProvider.portstr;

                        if(SimpleDynamoProvider.getSuccessor1(rec[1]).equals(port)
                                ||SimpleDynamoProvider.getpre1(rec[1]).equals(port)){
                            ContentValues cv = new ContentValues();
                            System.out.println("receiver recover data  " + rec[1]+"data"+rec[2]+" : "+rec[3]);
                            String key="insert" + "&&" + rec[2];
                            cv.put(COLUMN_NAME_KEY, key);
                            cv.put(COLUMN_NAME_VALUE, rec[3]);
                            SimpleDynamoProvider.put(cv);
                        }
                    }
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}



