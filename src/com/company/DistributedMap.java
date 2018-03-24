package com.company;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;

import java.util.stream.Collectors;


public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {
    private JChannel channel;
    private final HashMap<String, String> hashMap = new HashMap<>();

    public DistributedMap() throws Exception {


        channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);


        stack.addProtocol(new UDP())
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL()
                        .setValue("timeout", 12000)
                        .setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE());

        stack.init();


    }

    public void connect(String channelName) throws Exception {
        channel.connect(channelName);
        channel.getState(null, 0);

    }

    public void send(String str) {

        Message msg = new Message(null, null, str);
        try {
            channel.send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void close() {
        channel.close();
    }

    public void receive() {
        channel.setReceiver(new ReceiverAdapter() {
            @Override
            public void viewAccepted(View view) {
                super.viewAccepted(view);
                System.out.println(view.toString());
            }

            public void receive(Message msg) {
                if (messageFromOtherUser(msg)) {
                    processMessage(msg.getObject());
                    System.out.println("received msg from "
                            + msg.getSrc() + ": "
                            + msg.getObject());
                }
            }

            @Override
            public void getState(OutputStream output) throws Exception {
                synchronized (hashMap){
                List<String> str = hashMap.entrySet().stream()
                        .map(x -> "put "+x.getKey()+" "+x.getValue())
                        .collect(Collectors.toList());
                Util.objectToStream(str, new DataOutputStream(output));
                }
            }


            @Override
            public void setState(InputStream input) throws Exception {


        synchronized (hashMap) {
//            Set<Map.Entry<String, String>> entrySet;
//            entrySet = (Set<Map.Entry<String, String>>) Util.objectFromStream(new DataInputStream(input));
//            entrySet.forEach(x -> hashMap.put(x.getKey(), x.getValue()));
//            entrySet.forEach(System.out::println);
            List<String> list;
            list=(List<String>)Util.objectFromStream(new DataInputStream(input));
            list.forEach(x->processMessage(x));
        }
            }


        });
    }

    private void processMessage(Object msg) {
        if (msg instanceof String) {
            String message = (String) msg;
            if (message.startsWith("put")) {
                String key = message.split(" ")[1];
                String value = message.split(" ")[2];
                hashMap.put(key, value);
            } else if (message.startsWith("remove")) {
                String key = message.split(" ")[1];
                hashMap.remove(key);

            }

        }
    }

    private boolean messageFromOtherUser(Message msg) {
        return !msg.getSrc().equals(channel.getAddress());
    }

    @Override
    public boolean containsKey(String key) {
        return hashMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return hashMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        String result = hashMap.put(key, value);
        send("put " + key + " " + value);
        return result;
    }

    @Override
    public String remove(String key) {
        String result = hashMap.remove(key);
        send("remove " + key);
        return result;
    }

    public String getState() {
        StringBuilder sb = new StringBuilder();
        hashMap.entrySet()
                .forEach(entry -> sb.append(entry).append("\n"));
        return sb.toString();
    }





}
