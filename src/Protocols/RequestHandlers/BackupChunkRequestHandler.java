package Protocols.RequestHandlers;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;
import UDPMessengers.UDPMulticastMessenger;
import Utils.CommonUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class BackupChunkRequestHandler {
    protected String chunkId;
    protected String senderId;
    private String fileId;
    private String chunkNumber;
    private int replicationDegree;
    private byte[] chunkData;


    public BackupChunkRequestHandler(String senderId, String fileId, String chunkNumber, int replicationDegree, byte[] chunkData) {
        this.senderId = senderId;
        this.fileId = fileId;
        this.chunkNumber = chunkNumber;
        this.replicationDegree = replicationDegree;
        this.chunkData = chunkData;
        chunkId = this.fileId + "-" + this.chunkNumber;
    }

    public boolean backupChunk() {
        //check chunk is not from itself
        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return false;

        //check if chunk is not already backed up in this peer
        if (ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunkById(chunkId) != null)
            return false;

        //check peer has enough space to store the chunk
        if (ServerlessDistributedBackupService.systemData.getAvailableSpaceForBackup() < chunkData.length)
            return false;

        Chunk chunk = new Chunk(chunkNumber, fileId, senderId, chunkData.length, replicationDegree);
        chunk.incrementCurrReplicationDegree();
        ServerlessDistributedBackupService.systemData.addToCommunityBackedUpChunks(chunk);

        if (ServerlessDistributedBackupService.version.equals("2.0"))
            ServerlessDistributedBackupService.chunkIdToBackupChunkRequestHandler.put(chunkId, this);


        saveChunkDataInDisk();
        try {
            //Wait a randomly delay plus a factor multiplied by the currently occupied space, so that peers that have their space more filled, reply later tan peers that have less space filled
            //Since a peer must receive permission to keep the chunk and the first ones to say "STORED" are the ones hat will be keeping the file, this ensures that the peers with more free space are the ones who get to keep the file
            //Thus, contributing to better uniform distribution of space
            long occupationFactor = ServerlessDistributedBackupService.systemData.getSystemOccupationPercentage() / 100;
            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 50) + 500 * occupationFactor);
            sendStoredMessage();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;
    }

    private void saveChunkDataInDisk() {
        String filename = chunkId;
        String filepath = ServerlessDistributedBackupService.chunksPath + filename;

        try {
            FileOutputStream outputStream = new FileOutputStream(filepath);
            outputStream.write(chunkData);
            outputStream.close();
        } catch (IOException ex) {
            System.out.println("Error writing file '" + filepath + "'");
            ex.printStackTrace();
        }
    }

    private void sendStoredMessage() {
        String storedMessage = "STORED " + ServerlessDistributedBackupService.version + " " + ServerlessDistributedBackupService.serverId
                + " " + fileId + " " + chunkNumber + " \r\n\r\n";

        try {
            CommonUtils.waitRandomTimeMiliseconds(0, 400);
            UDPMulticastMessenger multicastSocket = new UDPMulticastMessenger(ServerlessDistributedBackupService.MC_Address,
                    ServerlessDistributedBackupService.MC_Port);
            multicastSocket.SendMessage(storedMessage.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}