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

import java.util.Arrays;

public class Channel {
    private  JChannel channel;

    public Channel() throws Exception {
        System.setProperty("java.net.preferIPv4Stack","true");

        channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);



        stack.addProtocol(new UDP())
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
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

        channel.connect("operation");

        System.out.println(channel.getAddressAsString());


    }

    public void send(String str){


        byte[] buffer = str.getBytes();
        Message msg = new Message(null, null, buffer);
        try {
            channel.send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void close(){
        channel.close();
    }

    public void receive(){
        channel.setReceiver(new ReceiverAdapter(){
            @Override
            public void viewAccepted(View view) {
                super.viewAccepted(view);
                System.out.println(view.toString());
            }
            public void receive(Message msg) {
                if(! msg.getSrc().equals(channel.getAddress()))
                    System.out.println("received msg from "
                            + msg.getSrc() + ": "
                            + new String(msg.getBuffer()));
            }
        });
    }
}
