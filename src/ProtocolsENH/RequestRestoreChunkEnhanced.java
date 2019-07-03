package ProtocolsENH;

import Protocols.Chunk;
import Protocols.Requesters.RequestRestoreChunk;
import Protocols.ServerlessDistributedBackupService;
import TCPMessengers.TCPServerMessenger;
import Utils.ByteUtils;

import java.io.IOException;
import java.util.Arrays;

public class RequestRestoreChunkEnhanced extends RequestRestoreChunk {
    private Integer tcpPortToReceiveChunk;
    private Integer timeoutInSeconds = 20;
    private byte[] data;

    public RequestRestoreChunkEnhanced(Chunk chunk) {
        super(chunk);
    }

    @Override
    public void run() {
        getAvailableTCPPort();
        sendRestoreRequestToMulticastSocket();
        TCPServerMessenger messenger = new TCPServerMessenger(tcpPortToReceiveChunk, timeoutInSeconds);
        try {
            messenger.acceptConnection();
        } catch (IOException e) {
            System.out.println("Request timed out for chunk " + chunk.getChunkId() + " !");
            return;
        }
        readMessageFromSocket(messenger);
        removeMessageHeader();
        putPortBackInAvailablePorts();
        saveChunkDataInDisk(data);
    }

    private void readMessageFromSocket(TCPServerMessenger messenger) {
        data = messenger.readMessage();
        messenger.closeSocket();
    }

    private void removeMessageHeader() {
        int delimiterPos = ByteUtils.indexOf(data, "\r\n\r\n");
        data = Arrays.copyOfRange(data, delimiterPos + 4, data.length);
    }

    private void putPortBackInAvailablePorts() {
        try {
            ServerlessDistributedBackupService.availablePorts.put(tcpPortToReceiveChunk);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected String buildRestoreRequestMessage() {
        String requestMessage = super.buildRestoreRequestMessage();
        requestMessage += " " + tcpPortToReceiveChunk.toString();
        requestMessage += " \r\n\r\n";
        return requestMessage;
    }

    private void getAvailableTCPPort() {
        try {
            tcpPortToReceiveChunk = ServerlessDistributedBackupService.availablePorts.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
