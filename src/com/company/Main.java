package com.company;

public class Main {

    public static void main(String[] args) throws Exception {
	Channel channel = new Channel();
	channel.receive();
	Thread.sleep(10000);

	channel.send("dupa");

	Thread.sleep(10000);

        channel.send("dupa");

        Thread.sleep(10000);

        channel.send("dupa");

        Thread.sleep(10000);

        channel.send("dupa");

        Thread.sleep(10000);

        channel.send("dupa");

        Thread.sleep(10000);

        channel.close();

    }
}
