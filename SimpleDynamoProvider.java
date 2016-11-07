package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.lang.String;
import java.util.HashMap;
import java.util.concurrent.SynchronousQueue;
import android.app.Activity;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	public static final String DATABASE_NAME = "GroupMessenger";
	public static final String COLUMN_NAME_KEY = "key";
	public static final String TABLE_NAME = "messages";
	public static final String COLUMN_NAME_VALUE = "value";
	static final String TAG =SimpleDynamoActivity.class.getSimpleName();
	private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	public static final Uri CURI = Uri.parse("content://" + AUTHORITY);
	SQLiteDatabase db;
	static String portstr;
	static String myPort;
	static String portnu;
	private static String successor1;
	private static String pre1;
	private static String pre2;

	private static String successor2;
	static HashMap<String,String> store1=new HashMap<String, String>();
	static HashMap<String,String> backup=new HashMap<String, String>();
	static HashMap<String,String> store2=new HashMap<String, String>();
	static HashMap<String,String> localstore=new HashMap<String, String>();
	static HashMap<String,String> replicastore1=new HashMap<String, String>();
	static HashMap<String,String> replicastore2=new HashMap<String, String>();

	class SimpleDynamoSQLiteOpenHelper extends SQLiteOpenHelper {
		public static final String DATABASE_NAME = "GroupMessenger";
		public static final String COLUMN_NAME_KEY = "key";
		public static final String TABLE_NAME = "messages";
		public static final String COLUMN_NAME_VALUE = "value";

		private static final String TYPE = "TEXT";
		public static final int VERSION= 1;
		public SimpleDynamoSQLiteOpenHelper(Context context) {
			super(context, DATABASE_NAME, null, VERSION);
		}
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
			db.execSQL("create table " + TABLE_NAME + " (" +COLUMN_NAME_KEY + " TEXT PRIMARY KEY," +COLUMN_NAME_VALUE + " " + TYPE  + ");");
		}
		public void onUpgrade(SQLiteDatabase db1, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
			onCreate(db);
		}
	}
	SimpleDynamoSQLiteOpenHelper mDbHelper;
	static Boolean write1=false;
	public static HashMap<String, Socket >sockettable =new HashMap<String, Socket>();

	@Override
	public boolean onCreate(){

		mDbHelper = new SimpleDynamoSQLiteOpenHelper(getContext());
		portstr=getport();
		portnu=getport();
		Thread channelThread = new Thread(new server());
		channelThread.start();

		System.out.println("SimpleDynamoProvider start :");

		String sec[]={"created"};
		Cursor res=mDbHelper.getReadableDatabase().rawQuery("select * from "+TABLE_NAME+" where "+ COLUMN_NAME_KEY+"=?",sec);
		if(res.getCount()==0){
			ContentValues cv = new ContentValues();
			cv.put(COLUMN_NAME_KEY, "created");
			cv.put(COLUMN_NAME_VALUE, "created");
			insert(CURI,cv);
		}else {
			new Thread(new Client("recover"+"&&"+getport()+"\n")).start();
		}

//		System.out.println("SimpleDynamoProvider create a client and ServerThread :"+res);

		return true;
	}
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void recover(String port){

		for(String keyy:backup.keySet()){
			System.out.println("recover port " + portnu);

			String msgs="recdata"+"&&"+ portnu+ "&&" +keyy+"&&"+backup.get(keyy)+"\n";
			new Thread(new Client(msgs)).start();

		}

	}



	public static void insertstore(ContentValues values){
		String key=(String)values.get("key");
		String value=(String)values.get("value");
		System.out.println("container !!!!!! insert: " + values);
		backup.put(key, value);

	}
	public static int deletestore(String key){
		System.out.println("Serverthread start delete!!!!!! received: " + key);

		if(key.equals("*")){
			backup.clear();
		}
		if(key.equals("@")){
			for(String keyy:backup.keySet()){
				String prev_1=getpre1(portstr);
				String prev_2=getpre1(prev_1);
				if(sendport(keyy).equals(prev_1)|| sendport(keyy).equals((portstr)) ||sendport(keyy).equals(prev_2) ){
					backup.remove(keyy);
				}
			}
	    }
		if(key.equals("@r1")){
		replicastore1.clear();
	    }
	    if(key.equals("@r2")){
		replicastore2.clear();
	    }
	    if(key.equals("@s1")){
		store1.clear();
	    }
		if(key.equals("@s2")){
		store2.clear();
		}else {
			backup.remove(key);
		}
		return 0;
	}

	// if the key is in this node then remove the selection
	// from this node and the next two successors
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v("start delete:", selection);

		//clear all the table or local table
		if (selection.equals("*")){
			backup.clear();
//			localstore.clear();
//			replicastore1.clear();
//			replicastore2.clear();
			Thread delete=new Thread(new Client(selection));
			delete.start();
		}
		//clear local table
		if((selection).equals("@")){
			for(String key:backup.keySet()){
				String prev_1=getpre1(portstr);
				String prev_2=getpre1(prev_1);
				if(sendport(key).equals(prev_1)|| sendport(key).equals((portstr)) ||sendport(key).equals(prev_2) ){
					backup.remove(key);
				}
			}
		}
		if(!(selection).equals("@")&&!(selection).equals("*")){

		if(backup.containsKey(selection)){
			backup.remove(selection);

			String msg="delete1"+"&&"+selection;
			Thread delete=new Thread(new Client(msg));
			delete.start();
		}
	}

		return 0;
	}
	public static int put(ContentValues values){
		String key = (String) values.get("key");
		String value = (String) values.get("value");
		String[] lab = key.split("&&");
		System.out.println("container  put work: " + values );

		if (lab[0].equals("insert")) {
			String key1 = lab[1];
			backup.put(key1, value);

		} else {
			backup.put(key, value);
			String msg = "insert" + "&&" + key + "&&" + value+"\n";
			Thread insert = new Thread(new Client(msg));
			insert.start();

//			if (sendport(key).equals(portstr)) {
//
//				localstore.put(key, value);
//				System.out.println("container  insert sendout: " + values + "print localstore: " + localstore + "sendport: " + sendport(key) + "port number: " + portstr);
//
//			} else if (sendport(key).equals(getSuccessor1(portstr))) {
//				replicastore1.put(key, value);
//			System.out.println("container  insert sendout: " + values+"print replicastore1: " + replicastore1 + "sendport: " + sendport(key) + "port number: " + portstr);
//			String msg="insert"+"&&"+portstr+"&&"+key+"&&"+value;
//				String msg = "insert" + "&&" + key + "&&" + value;
//
//				Thread insert = new Thread(new Client(msg));
//				insert.start();
//			} else if (sendport(key).equals(getSuccessor2(portstr))) {
//				replicastore2.put(key, value);
////			System.out.println("container  insert sendout: " + values+"print replicastore2: " + replicastore2 + "sendport: " + sendport(key) + "port number: " +portstr);
//				String msg = "insert" + "&&" + key + "&&" + value;
//				Thread insert = new Thread(new Client(msg));
//				insert.start();
//			} else if (sendport(key).equals(getSuccessor1(getSuccessor2(portstr)))) {
//				store1.put(key, value);
//				String msg = "insert" + "&&" + key + "&&" + value;
//				Thread insert = new Thread(new Client(msg));
//				insert.start();
////			System.out.println("container  insert sendout: " + values + "print store1: " + store1 + "sendport: " + sendport(key) + "port number: " + portstr + getSuccessor1(getSuccessor2(portstr)));
//
//			} else if (sendport(key).equals(getSuccessor2(getSuccessor2(portstr)))) {
//				store2.put(key, value);
//				String msg = "insert" + "&&" + key + "&&" + value;
//				Thread insert = new Thread(new Client(msg));
//				insert.start();
//				System.out.println("container  insert sendout: " + values + "print store1: " + store2 + "sendport and getport" + sendport(key) + portstr + getSuccessor2(getSuccessor2(portstr)));

//			}
		}

		return 0;
	}

	@Override
	public synchronized  Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
//		Log.v("containprovider insert", values.toString());


		write1 = true;
		System.out.println("containprovider  insert: " + values);

		String key = (String) values.get("key");
		String value = (String) values.get("value");

		if(key.equals("created")){
			SQLiteDatabase db1 = mDbHelper.getReadableDatabase();
//        //mDbHelper.onUpgrade(db,1,2);
			String new_rowid = (String) values.get("key");
			String sec[] = {new_rowid};

			Cursor res = mDbHelper.getReadableDatabase().rawQuery("select * from " + TABLE_NAME + " where " + COLUMN_NAME_KEY + "=?", sec);

			// must keep in mind that to check if is null must using getCount()!!!!
			if (res.getCount() == 0) {
				db1.insert("messages", null, values);
			} else {
				db1.update("messages", values, "key=?", sec);
			}
		}else {
			put(values);
		}
		write1=false;
		notifyAll();

		return uri;
	}
//synchronized
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Cursor res=null;
		Cursor result=null;
		String[] collection= new String[]{COLUMN_NAME_KEY,COLUMN_NAME_VALUE};
		MatrixCursor resultcursor = new MatrixCursor(collection);


		Log.v("start query: ", selection);
		if((selection).equals("*")){
			int i=0;
			//System.out.println("return  query waiting... ");
			while(write1){
//				System.out.println("return  query waiting... ");
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			for(String key:backup.keySet()) {
				String[] colvalues=new String[2];
				colvalues[0]=key;
				colvalues[1]=backup.get(key);
				resultcursor.addRow(colvalues);
			}
		}



		if((selection).equals("@")){
			int i=0;

			while(write1){
				//System.out.println("local return  query waiting... ");

				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			for(String key:backup.keySet()){
				String[] colvalues=new String[2];
				colvalues[0]=key;
				colvalues[1]=backup.get(key);
                String prev_1=getpre1(portstr);
				String prev_2=getpre1(prev_1);
				if(sendport(key).equals(prev_1)|| sendport(key).equals((portstr)) ||sendport(key).equals(prev_2) ){
					resultcursor.addRow(colvalues);
				}
			}
		}
		if(!selection.equals("@")&&!selection.equals("*")) {
			while(write1){
				//System.out.println("return  query waiting... ");
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
				String[] colvalues=new String[2];
				colvalues[0]=selection;
				colvalues[1]=backup.get(selection);
				resultcursor.addRow(colvalues);
		}

		System.out.println("return  query of: " + selection + " is: " + resultcursor.getCount());
		return resultcursor ;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

	public  String getport(){
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portstr=tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return portstr;
	}

	public static String getpre1(String portstr) {
		if(portstr.equals("5562")){
			pre1="5560";
		}
		if(portstr.equals("5556")){
			pre1="5562";
		}
		if(portstr.equals("5554")){
			pre1="5556";
		}
		if(portstr.equals("5558")){
			pre1="5554";
		}
		if(portstr.equals("5560")){
			pre1="5558";
		}
		return pre1;
	}
	public static String getpre2(String portstr) {
		if(portstr.equals("5562")){
			pre2="5558";
		}
		if(portstr.equals("5556")){
			pre2="5560";
		}
		if(portstr.equals("5554")){
			pre2="5562";
		}
		if(portstr.equals("5558")){
			pre2="5556";
		}
		if(portstr.equals("5560")){
			pre2="5554";
		}
		return pre2;
	}

	public static String getSuccessor1(String portstr) {
		if(portstr.equals("5562")){
			successor1="5556";
		}
		if(portstr.equals("5556")){
			successor1="5554";
		}
		if(portstr.equals("5554")){
			successor1="5558";
		}
		if(portstr.equals("5558")){
			successor1="5560";
		}
		if(portstr.equals("5560")){
			successor1="5562";
		}
		return successor1;
	}


	public static String getSuccessor2(String portstr) {
		if(portstr.equals("5562")){
			successor2="5554";
		}
		if(portstr.equals("5556")){
			successor2="5558";
		}
		if(portstr.equals("5554")){
			successor2="5560";
		}
		if(portstr.equals("5558")){
			successor2="5562";
		}
		if(portstr.equals("5560")){
			successor2="5556";
		}
		return successor2;
	}

//	177ccecaec32c54b82d5aaafc18a2dadb753e3b1=5562,1 11124 5
//	208f7f72b198dadd244e61801abe1ec3a4857bc9=5556,2 11112 2
//	33d6357cfaaf0f72991b0ecd8c56da066613c089=5554,3 11108 1
//	abf0fd8db03e5ecb199a9b82929e9db79b909643=5558,4 11116 3
//	c25ddd596aa7c81fa12378fa725f706d54325d12=5560,5 11120 4

// 177ccecaec32c54b82d5aaafc18a2dadb753e3b1,
// 208f7f72b198dadd244e61801abe1ec3a4857bc9,
// 33d6357cfaaf0f72991b0ecd8c56da066613c089,
// abf0fd8db03e5ecb199a9b82929e9db79b909643,
// c25ddd596aa7c81fa12378fa725f706d54325d12


	public static String sendport(String key){
		String hashkey=null;
		String sendport;
		sendport = null;
		String port1=null;
		String port2=null;
		String port3=null;
		String port4=null;
		String port5=null;
		try {
			hashkey=genHash(key);
			port1=genHash("5562");
			port2=genHash("5556");
			port3=genHash("5554");
			port4=genHash("5558");
			port5=genHash("5560");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


//			if(hashkey.compareTo(port1)<=0||(hashkey.compareTo(port5)>0)){
//                sendport="5562";
//            }
			if((hashkey.compareTo(port1)>0)&&hashkey.compareTo(port2)<=0 ){
				sendport="5556";
			}
			else if((hashkey.compareTo(port2)>0)&&hashkey.compareTo(port3)<=0){
				sendport="5554";
			}
			else if((hashkey.compareTo(port3)>0)&&hashkey.compareTo(port4)<=0){
				sendport="5558";
			}
			else if((hashkey.compareTo(port4)>0)&&hashkey.compareTo(port5)<=0){
				sendport="5560";
			}
			else {
				sendport="5562";

			}

		return  sendport;
	}


	private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}