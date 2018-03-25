package com.company;

import org.jgroups.*;
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
    private final JChannel channel;

    public MapReceiver(Map<String, String> hashMap, JChannel channel) {
        this.hashMap = hashMap;
        this.channel = channel;
    }

    @Override
    public void viewAccepted(View view) {
        handleView(channel, view);
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
            hashMap.clear();
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
        else if(msg instanceof Map.Entry){
            Map.Entry<String,String> entry = (Map.Entry<String,String>) msg;
            hashMap.put(entry.getKey(),entry.getValue());
        }
    }

    private static void handleView(JChannel ch, View new_view) {
        System.out.println(new_view.toString());
        if (new_view instanceof MergeView) {
            ViewHandler handler = new ViewHandler(ch, (MergeView) new_view);
            // requires separate thread as we don't want to block JGroups
            handler.start();
        }
    }

    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch = ch;
            this.view = view;
        }

        public void run() {
            List<View> subgroups = view.getSubgroups();
            View tmp_view = subgroups.get(0); // picks the first
            Address local_addr = ch.getAddress();
            System.out.println("\n\nmoj adres to" + ch.getAddress() + "\n\n");
            if (!tmp_view.containsMember(local_addr)) {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will re-acquire the state");
                try {
                    ch.getState(null, 30000);
                } catch (Exception ex) {
                }
            } else {
                System.out.println("Member of the new primary partition ("
                        + tmp_view + "), will do nothing");
            }
        }
    }
}
