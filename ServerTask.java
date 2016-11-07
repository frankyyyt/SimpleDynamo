package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by fengguotian on 4/25/16.
 */

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;




//public class Client extends Thread {

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    //initialize the stature prepare for failure detection

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String COLUMN_NAME_KEY = "key";
    static final String TABLE_NAME = "messages";
    static final String COLUMN_NAME_VALUE = "value";
    Context content;
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;

    private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CURI = Uri.parse("content://" + AUTHORITY);

    static Boolean receiverooting=false;
    public static String REMOTE_PORT = "11108";//"11112","11116","11120", "11124"};
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
//        public void run() {
//

        while (true)

            try {

                ServerSocket serverSocket =new ServerSocket(SERVER_PORT);;//sockets[0];
//                  //Waits for an incoming request and blocks until the connection is opened.
                serverSocket.setReuseAddress(true);
                Socket socket = serverSocket.accept();
                String[] rec;
                //Returns an input stream to read data from this socket.
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String strings = in.readLine();
                System.out.println(strings);
                rec = strings.split("&&");
                Log.v("Start receive: ", strings);
                // the port number sequerence number and message
                String post = null;
                if (!strings.isEmpty()){
                    String msgToSend;
//    String msgs=type+"&&"+recport+"&&"+myport+"&&+precessor+"&&"+successor+"&&"+queryport"&&"+labs+"&&"+insertvalue;

                    if(rec[0].equals("returnquery")){
                        String rekey=rec[2];
                        String revalue=rec[3];
                        MatrixCursor returncur = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});
                        System.out.println("return query " );
                        returncur.addRow(new Object[]{rekey, revalue});
                    }

                    //  msg ="query"+"&&"+ preid+"&&" + myPort+"&&"+selection; /*/@
                    if (rec[0].equals("insert")){
                        //msg = "insert" + "&&" + sucid + "&&" + myPort + "&&" + key+"&&"+value;
//                        String[] rec=strReceived.split("&&");
//        ContentValues cv = new ContentValues();
//        cv.put(COLUMN_NAME_KEY, rec[1]);
//        cv.put(COLUMN_NAME_VALUE, rec[2]);
//        content.getContentResolver().insert(CURI, cv);


                    }


                    if (rec[0].equals("query")) {
                        String queryback;
//                    msg = "query" + "&&" + sucid + "&&" + myPort + "&&" + selection;
                        ///              rec[2]= the original node
                        post = "query" + "&&" +rec[2]+"&&"+ rec[3];
                        {
                            Cursor result = content.getContentResolver().query(CURI, null, rec[3], null, null);
                            int keyIndex = result.getColumnIndex(KEY_FIELD);
                            int valueIndex = result.getColumnIndex(VALUE_FIELD);
                            String returnKey = (String) result.getString(keyIndex);
                            String returnValue = (String) result.getString(valueIndex);
                            String send = "returnquery" + "&&" + rec[1] + "&&" + returnKey + "&&" + returnValue;
                            ///not just next need to find the successor
                            Thread thread = new Thread(new Client(send));
                            thread.start();
                        }
                        if(rec[3].compareTo(rec[1])>0){
//                            String getsuc = SimpleDynamoProvider.sucid;
                            //if next port then send to next port
                            String send = "query" +"&&"+rec[2]+"&&"+ rec[3];
//                        post = "query" + "&&" + new chord().dhtch.sucid + "&&"  + selection;
                            ///not just next need to find the successor
                            Thread thread = new Thread(new Client(send));
                            thread.start();
                        }
                    }
                    //  msg ="query"+"&&"+ preid+"&&" + myPort+"&&"+selection; /*/@
                    if (rec[0].equals("delete")) {
                        post = "delete" + "&&" + rec[3];
//                    content.getContentResolver().delete(CURI, rec[3], null);
                    }
                    else if(rec[0].equals("delete")){
                        content.getContentResolver().delete(CURI, rec[1], null);
                    }
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
    }

    protected void onPostExecute(String strings) {

        String strReceived = strings.trim();
        System.out.println("progress update: " + strReceived);
        String[] rec=strReceived.split("&&");
//        ContentValues cv = new ContentValues();
//        cv.put(COLUMN_NAME_KEY, rec[1]);
//        cv.put(COLUMN_NAME_VALUE, rec[2]);
//        content.getContentResolver().insert(CURI, cv);

    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}



