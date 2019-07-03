package Protocols.Requesters;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;
import UDPMessengers.UDPMulticastMessenger;

import java.io.IOException;

public class RemoveChunkFromReclaimProtocol implements Runnable {
    private String senderId;
    private String fileId;
    private String chunkNumber;
    private String removedMessage;


    public RemoveChunkFromReclaimProtocol(Chunk chunk) {
        this.senderId = chunk.getSenderId();
        this.fileId = chunk.getFileId();
        this.chunkNumber = chunk.getNumber();
    }

    @Override
    public void run() {
        createRemovedMessage();
        sendRemovedMessage();

    }

    private void createRemovedMessage() {
        removedMessage = "REMOVED " + ServerlessDistributedBackupService.version + " " +
                senderId + " " + fileId + " " + chunkNumber + " \r\n\r\n";
    }

    private void sendRemovedMessage() {
        try {
            UDPMulticastMessenger udpMulticastMessenger = new UDPMulticastMessenger(ServerlessDistributedBackupService.MC_Address,
                    ServerlessDistributedBackupService.MC_Port);
            udpMulticastMessenger.SendMessage(removedMessage.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
