package Protocols.Requesters;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;
import UDPMessengers.UDPMulticastMessenger;

import java.io.FileOutputStream;
import java.io.IOException;

public class RequestRestoreChunk implements Runnable {
    protected Chunk chunk;
    protected UDPMulticastMessenger multicastSocket;
    public final Object mutex = new Object();
    private int waitingTimeForChunk = 2000; //miliseconds

    public RequestRestoreChunk(Chunk chunk) {
        this.chunk = chunk;
    }

    @Override
    public void run() {
        sendRestoreRequestToMulticastSocket();

        //add mapping from chunk id that is being requested to this thread (responsible for handling it), so that when the response arrives, this thread will be notified by calling of the method chunkArrived
        ServerlessDistributedBackupService.chunkIdToRequestRestoreThread.put(chunk.getChunkId(), this);

        synchronized (mutex) {
            try {
                mutex.wait(waitingTimeForChunk);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    protected void sendRestoreRequestToMulticastSocket() {
        String requestMessage = buildRestoreRequestMessage();
        requestMessage = appendTerminationToMessage(requestMessage);
        byte[] message = requestMessage.getBytes();
        try {
            multicastSocket = new UDPMulticastMessenger(ServerlessDistributedBackupService.MDB_Address, ServerlessDistributedBackupService.MDB_Port);
            multicastSocket.SendMessage(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected String buildRestoreRequestMessage() {
        String message = "GETCHUNK " + ServerlessDistributedBackupService.version + " " + chunk.getSenderId() +
                " " + chunk.getFileId() + " " + chunk.getNumber() + " \r\n";

        return message;
    }

    public void chunkArrived(String senderId, byte[] chunkData) {
        //do nothing if chunk is from himself
        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return;

        saveChunkDataInDisk(chunkData);
        System.out.println("Saved chunk " + chunk.getChunkId() + " data in disk");
        // inform thread that chunk arrived and has been successfully saved
        synchronized (mutex) {
            mutex.notify();
        }
    }

    protected void saveChunkDataInDisk(byte[] chunkData) {
        String filename = chunk.getChunkId();
        String filepath = ServerlessDistributedBackupService.restoredChunksPath + filename;

        try {
            FileOutputStream outputStream = new FileOutputStream(filepath);
            outputStream.write(chunkData);
            outputStream.close();
        } catch (IOException ex) {
            System.out.println("Error writing file '" + filepath + "'");
            ex.printStackTrace();
        }
    }

    protected String appendTerminationToMessage(String message) {
        return message + "\r\n";
    }

}
