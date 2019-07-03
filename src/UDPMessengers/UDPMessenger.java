package UDPMessengers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;

public class UDPMessenger {
    protected DatagramSocket socket;
    protected InetAddress destinationAdress;
    protected int destinationPort;
    protected InetAddress sourceIpAdressOfLastReceivedPacket;


    public InetAddress getSourceIpAdressOfLastReceivedPacket() {
        return sourceIpAdressOfLastReceivedPacket;
    }

    public void SendMessage(byte[] message) throws IOException {
        DatagramPacket packet = new DatagramPacket(message, message.length, destinationAdress, destinationPort);
        socket.send(packet);
    }

    public void SendMessage(String message) throws IOException {
        byte[] messageByteArray = message.getBytes();
        this.SendMessage(messageByteArray);
    }

    public byte[] ReceiveMessage() throws IOException {
        //read message from socket
        byte[] rbuf = new byte[64 * 1024];
        DatagramPacket packet = new DatagramPacket(rbuf, rbuf.length);
        socket.receive(packet);
        byte[] data = Arrays.copyOfRange(rbuf, 0, packet.getLength());

        sourceIpAdressOfLastReceivedPacket = packet.getAddress();

        return data;
    }

    public String ReceiveMessageString() throws IOException {
        String message = new String(this.ReceiveMessage());
        message = message.trim();

        return message;
    }

}
