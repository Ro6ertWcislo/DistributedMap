package com.company;

import org.jgroups.*;

import java.util.HashMap;


public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {
    private MapChannel channel;
    private final HashMap<String, String> hashMap = new HashMap<>();

    public DistributedMap() throws Exception {
        channel = new MapChannel();
        channel.setReceiver(hashMap);
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
//        String result = hashMap.put(key, value);
        send("put " + key + " " + value);
        return value;
    }

    @Override
    public String remove(String key) {
        String result = hashMap.get(key);
        send("remove " + key);
        return result;
    }

    public String getState() {
        StringBuilder sb = new StringBuilder();
        hashMap.entrySet()
                .forEach(entry -> sb.append(entry).append("\n"));
        return sb.toString();
    }


    public void connect(String channelName) throws Exception {
        channel.connect(channelName);
    }

    private void send(String msg) {
        channel.send(msg);
    }

    public void close() {
        channel.close();
    }


}
