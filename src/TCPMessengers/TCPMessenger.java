package TCPMessengers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

public class TCPMessenger {
    protected Socket socket;

    public byte[] readMessage() {
        byte[] messageBytes = new byte[64 * 1024];
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int currIndex = 0;
            int sizeRead;

            while ((sizeRead = in.read(messageBytes, currIndex, 64 * 1024 - currIndex)) != -1)
                currIndex += sizeRead;

            if (currIndex != 64 * 1024) {
                byte[] data = Arrays.copyOfRange(messageBytes, 0, currIndex);
                messageBytes = data;
            }
        } catch (IOException e) {
            //Socket closed, all data has been read and can be returned
        }

        return messageBytes;
    }

    public void writeMessage(byte[] messageBytes) {
        try {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.write(messageBytes);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeSocket() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
