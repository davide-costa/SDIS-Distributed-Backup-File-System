package TCPMessengers;


import java.io.IOException;
import java.net.ServerSocket;

public class TCPServerMessenger extends TCPMessenger {
    private ServerSocket serverSocket;

    public TCPServerMessenger(int port) {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TCPServerMessenger(int port, int timeoutInSeconds) {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(timeoutInSeconds * 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void acceptConnection() throws IOException {
        socket = serverSocket.accept();
    }

    public void closeSocket() {
        super.closeSocket();

        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
