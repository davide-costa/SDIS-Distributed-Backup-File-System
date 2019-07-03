package ChannelListenners;

import UDPMessengers.UDPMessenger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChannelListener implements Runnable {
    protected UDPMessenger messenger;
    ExecutorService executor;

    public ChannelListener(UDPMessenger messenger) {
        this.messenger = messenger;
        executor = Executors.newFixedThreadPool(150);
    }

    @Override
    public void run() {
        while (true) {
            try {
                byte[] receivedBytes = messenger.ReceiveMessage();
                InetAddress sourceIpAdress = messenger.getSourceIpAdressOfLastReceivedPacket();
                ChannelDispatcher dispatcher = new ChannelDispatcher(receivedBytes, sourceIpAdress);
                executor.execute(dispatcher);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
