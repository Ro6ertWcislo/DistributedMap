package com.company;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;

import java.util.concurrent.ConcurrentHashMap;


public class DistributedMap implements SimpleStringMap {
    private JChannel channel;
    private ConcurrentHashMap<String,String> concurrentMap= new ConcurrentHashMap<>();

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
                .addProtocol(new FRAG2());

        stack.init();



    }

    public void connect(String channelName) throws Exception {
        channel.connect(channelName);
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
                if (messageFromOtherUser(msg)){
                    processMessage(msg.getObject());
                    System.out.println("received msg from "
                            + msg.getSrc() + ": "
                            + msg.getObject());
                }
            }
        });
    }
    private void processMessage(Object msg){
        if(msg instanceof String){
            String message = (String) msg;
            if(message.startsWith("put")){
                String key = message.split(" ")[1];
                String value = message.split(" ")[2];
                concurrentMap.put(key,value);
            }
            else if(message.startsWith("remove")){
                String key = message.split(" ")[1];
                concurrentMap.remove(key);

            }

    }
    }

    private boolean messageFromOtherUser(Message msg) {
        return !msg.getSrc().equals(channel.getAddress());
    }

    @Override
    public boolean containsKey(String key) {
        return concurrentMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return concurrentMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        String result = concurrentMap.put(key,value);
        send("put " + key + " "+ value);
        return result;
    }

    @Override
    public String remove(String key) {
        String result = concurrentMap.remove(key);
        send("remove " + key);
        return result;
    }
}
