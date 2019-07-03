package UDPMessengers;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UDPUnicastMessenger extends UDPMessenger {
    protected int listenPort;

    public UDPUnicastMessenger(int destinationPort, int listenPort) throws IOException {
        this.listenPort = listenPort;
        this.destinationPort = destinationPort;
        this.socket = new DatagramSocket(this.listenPort);
    }

    public UDPUnicastMessenger(InetAddress destinationAdress, int destinationPort, int listenPort) throws IOException {
        this(destinationPort, listenPort);
        this.destinationAdress = destinationAdress;
    }

    public UDPUnicastMessenger(String destinationAdressStr, int destinationPort, int listenPort) throws IOException {
        this(InetAddress.getByName(destinationAdressStr), destinationPort, listenPort);
    }
}
