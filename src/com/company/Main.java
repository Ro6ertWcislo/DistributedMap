package com.company;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack","true");
        String channel = "operation";
        DistributedMap distributedMap = new DistributedMap(channel);


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
                System.out.println(distributedMap.containsKey(msg.substring(3)));
            }else if(msg.equals("state")){
                System.out.println(distributedMap.getState());
            }
            else if (!msg.equals("close"))
                System.out.println("unknown command. Try again");
        }

    }
}
