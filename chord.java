package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

/**
 * Created by fengguotian on 4/25/16.
 */

/* Class chord */
public class chord {

    public static int size=0;
    static HashMap<String, String> ring=new HashMap<String, String>(); //the key is the hashport and the value is the port number like 11108
    public static ArrayList<String> lookup=new ArrayList<String>();
    static String firstport;
    static String lastport;

    public void add(String port) throws NoSuchAlgorithmException {
        if(port!=null){
            String hashport=genHash(String.valueOf(Integer.parseInt(port) / 2));
            ring.put(hashport,port);
            lookup.add(hashport);
            Collections.sort(lookup);//in an increasing order of the hashed port id
            size++ ;
            Log.v("chord add ring node!!!!", lookup.toString() + size);
        }
    }

    public boolean contain(String portid) throws NoSuchAlgorithmException {
        String query=genHash(String.valueOf(Integer.parseInt(portid) / 2));
        boolean result=lookup.contains(query);
        return result;
    }

    //first hashed port number + port id
    public String getfirstport(){
        firstport= lookup.get(0)+"&&"+ring.get(lookup.get(0));
        return firstport;
    }
    public String getlasttport(){
        lastport=lookup.get(lookup.size()-1)+"&&"+ring.get(lookup.get(lookup.size()-1));
        return lastport;
    }

    public  String getprocess(String port) throws NoSuchAlgorithmException {
        String key;
        String value;
        String query=genHash(String.valueOf(Integer.parseInt(port) / 2));

        int index=lookup.indexOf(query);
//
        if (index==0&&size>=1){
            key= lookup.get(size-1);
        }else{
            key=lookup.get(index-1);
        }
        value=ring.get(key);
        System.out.println(value);

        String tempp=value+"&&"+key; //key is the hashed portid, value is the portid like "11108"
        return tempp;
    }

    public  String getsucess(String port) throws NoSuchAlgorithmException {
        HashMap<String, String> result=new HashMap<String, String>();
        String key;
        String value;
        String query=genHash(String.valueOf(Integer.parseInt(port) / 2));
        int index=lookup.indexOf(query);
        if(index==0&&size==1){
            key= lookup.get(index);
        }else if (index==size-1&&size>=1){
            key= lookup.get(0);
        }else{
            key=lookup.get(index+1);
        }
        value=ring.get(key);
//        result.put(key,value);
        String tempp=value+"&&"+key;  //value is the portid like "11108", key is the hashed portid,
        return tempp;
    }
    /* Function to get size of list */
    public int getSize() {
        return size;
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