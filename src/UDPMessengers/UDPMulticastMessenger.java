package UDPMessengers;


import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class UDPMulticastMessenger extends UDPMessenger {
    private UDPMulticastPeriodicBroadcaster broadcaster;
    private Thread broadcasterThread;

    public UDPMulticastMessenger(InetAddress address, int port, boolean joinGroupNow) throws IOException {
        this.destinationAdress = address;
        this.destinationPort = port;
        this.socket = new MulticastSocket(this.destinationPort);
        if (joinGroupNow)
            this.joinGroup(address);
    }

    public UDPMulticastMessenger(InetAddress address, int port) throws IOException {
        this(address, port, false);
    }

    public UDPMulticastMessenger(String address, int port) throws IOException {
        this(InetAddress.getByName(address), port, false);
    }

    public UDPMulticastMessenger(String address, int port, boolean joinGroupNow) throws IOException {
        this(InetAddress.getByName(address), port, joinGroupNow);
    }

    private void joinGroup(InetAddress address) throws IOException {
        ((MulticastSocket) this.socket).joinGroup(address);
    }

    public void startBroadcastingMessage(String message, int delay) {
        broadcaster = new UDPMulticastPeriodicBroadcaster(this, message, delay);
        broadcasterThread = new Thread(broadcaster);
        broadcasterThread.start();
    }

    public void stopBroadcastingMessage() {
        broadcaster.stopBroadcasting();
    }
}