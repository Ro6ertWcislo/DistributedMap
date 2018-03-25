package com.company;

import org.jgroups.*;

import java.util.HashMap;


public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {
    private MapChannel channel;
    private final HashMap<String, String> hashMap = new HashMap<>();

    public DistributedMap(String channelName) throws Exception {
        channel = new MapChannel();
        channel.setReceiver(hashMap);
        channel.connect(channelName);
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
        send("put " + key + " " + value);
        return value;
    }

    @Override
    public String remove(String key) {
        String result = hashMap.get(key);
        send("remove " + key);
        return result;
    }

    @Override
    public String getState() {
        StringBuilder sb = new StringBuilder();
        hashMap.entrySet()
                .forEach(entry -> sb.append(entry).append("\n"));
        return sb.toString();
    }



    private void send(String msg) {
        channel.send(msg);
    }



}
