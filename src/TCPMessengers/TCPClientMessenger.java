package TCPMessengers;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;

public class TCPClientMessenger extends TCPMessenger {
    public TCPClientMessenger(InetAddress address, int port) throws ConnectException {
        try {
            socket = new Socket(address, port);
        } catch (IOException e) {
            if (e instanceof ConnectException) {
                throw new ConnectException();
            }
            e.printStackTrace();
        }
    }

    public TCPClientMessenger(String address, int port) throws ConnectException {
        try {
            socket = new Socket(address, port);
        } catch (IOException e) {
            if (e instanceof ConnectException) {
                throw new ConnectException();
            }
            e.printStackTrace();
        }
    }
}
