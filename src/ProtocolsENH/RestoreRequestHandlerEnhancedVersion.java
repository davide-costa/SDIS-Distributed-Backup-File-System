package ProtocolsENH;


import Protocols.RequestHandlers.RestoreRequestHandler;
import TCPMessengers.TCPClientMessenger;

import java.net.ConnectException;

public class RestoreRequestHandlerEnhancedVersion extends RestoreRequestHandler {
    private int destinationPort;
    private String destinationAddress;


    public RestoreRequestHandlerEnhancedVersion(String fileId, String chunkNumber, String destinationAddress, int destinationPort) {
        super(fileId, chunkNumber);
        this.destinationPort = destinationPort;
        this.destinationAddress = destinationAddress;
    }

    public boolean handleRequest() {
        if (!super.handleRequest())
            return false;

        TCPClientMessenger tcpClientMessenger = null;
        try {
            tcpClientMessenger = new TCPClientMessenger(destinationAddress, destinationPort);
        } catch (ConnectException e) //connection timeout, possibly because server did not accept the connection, another peer arrived first
        {
            return false;
        }
        tcpClientMessenger.writeMessage(responseMessage);
        tcpClientMessenger.closeSocket();
        return true;
    }

}