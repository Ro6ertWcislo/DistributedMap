package com.company;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapReceiver extends ReceiverAdapter {
    private final Map<String, String> hashMap;

    public MapReceiver(Map<String, String> hashMap) {
        this.hashMap = hashMap;
    }

    @Override
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        System.out.println(view.toString());
    }

    public void receive(Message msg) {
        processMessage(msg.getObject());
        System.out.println("received msg from "
                + msg.getSrc() + ": "
                + msg.getObject());
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized (hashMap) {
            List<String> str = hashMap.entrySet().stream()
                    .map(x -> "put " + x.getKey() + " " + x.getValue())
                    .collect(Collectors.toList());
            Util.objectToStream(str, new DataOutputStream(output));
        }
    }


    @Override
    public void setState(InputStream input) throws Exception {


        synchronized (hashMap) {
            List<String> list;
            list = (List<String>) Util.objectFromStream(new DataInputStream(input));
            list.forEach(this::processMessage);
        }
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
}
