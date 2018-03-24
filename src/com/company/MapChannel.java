package com.company;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.ProtocolStack;

import java.net.InetAddress;
import java.util.Map;

public class MapChannel {
    private JChannel channel;

    public MapChannel() throws Exception {
        this.channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        initStack(stack);

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

    private void initStack(ProtocolStack stack) throws Exception {
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.1")))
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

    public void setReceiver(Map<String,String> hashMap) {
        channel.receiver(new MapReceiver(hashMap, channel));
    }


    }

