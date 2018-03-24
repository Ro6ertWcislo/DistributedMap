package com.company;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack","true");

        DistributedMap distributedMap = new DistributedMap();
//        new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.1"));
        String channel = "operation";
        distributedMap.receive();
        distributedMap.connect(channel);


        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(isr);
        String msg = "";

        while (!msg.equals("close")) {
            msg = br.readLine();
            if (msg.startsWith("put")) {
                String[] x = msg.split(" ");
                String key = x[1];
                String value = x[2];
                distributedMap.put(key, value);
            } else if (msg.startsWith("remove")) {
                distributedMap.remove(msg.substring(7));
            } else if (msg.startsWith("get")) {
                System.out.println(distributedMap.get(msg.substring(4)));
            } else if (msg.startsWith("ck")) {
                distributedMap.containsKey(msg.substring(3));
            } else if (msg.equals("con") || msg.equals("connect")) {
                distributedMap.connect(channel);
            } else if (msg.equals("dis") || msg.equals("disconnect")) {
                distributedMap.close();
            } else if(msg.equals("state")){
                System.out.println(distributedMap.getState());
            }
            else if (!msg.equals("close"))
                System.out.println("unknown command. Try again");
            else distributedMap.close();
        }

    }
}
