package ProtocolsENH;

import Protocols.Chunk;
import Protocols.RequestHandlers.BackupChunkRequestHandler;
import Protocols.ServerlessDistributedBackupService;
import Utils.FileUtils;


public class BackupChunkRequestHandlerEnhanced extends BackupChunkRequestHandler {
    private int waitingTimeForCanStoreMessage = 5000; // miliseconds
    private boolean canStoreMessageArrived = false;
    private final Object mutex = new Object();


    public BackupChunkRequestHandlerEnhanced(String senderId, String fileId, String chunkNumber, int replicationDegree, byte[] chunkData) {
        super(senderId, fileId, chunkNumber, replicationDegree, chunkData);
    }

    public boolean backupChunk() {
        if (!super.backupChunk())
            return false;
        waitForCanStoreMessageWithTimeout(waitingTimeForCanStoreMessage);
        if (!canStoreMessageArrived) {
            deleteChunkFromDisk();
            deleteChunkFromSystemData();
        }

        return true;
    }

    private void waitForCanStoreMessageWithTimeout(int timeout) {
        synchronized (mutex) {
            try {
                mutex.wait(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteChunkFromDisk() {
        String filename = chunkId;
        String filepath = ServerlessDistributedBackupService.chunksPath + filename;
        FileUtils.deleteFileFromDisk(filepath);
    }

    public void deleteChunkFromSystemData() {
        Chunk chunk = ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunkById(chunkId);
        ServerlessDistributedBackupService.systemData.removeChunkFromCommunityBackedUpChunks(chunk);
    }

    public void canStoreArrived(String senderId, String peerIdThatCanStoreTheChunk) {
        if (!senderId.equals(this.senderId))
            return;
        //Check if the peer who gets to store the chunk is this peer; if yes, flag variable accordingly; if not, discard event
        if (!peerIdThatCanStoreTheChunk.equals(ServerlessDistributedBackupService.serverId))
            return;
        canStoreMessageArrived = true;
        synchronized (mutex) {
            mutex.notify();
        }
    }

}
