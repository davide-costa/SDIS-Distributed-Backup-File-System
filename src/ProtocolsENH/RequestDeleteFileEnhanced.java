package ProtocolsENH;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;
import Utils.CommonUtils;

import java.util.List;

public class RequestDeleteFileEnhanced implements Runnable {
    private String fileId;
    private List<Chunk> chunksToRemove;
    private final Object mutex = new Object();


    public RequestDeleteFileEnhanced(String fileId) {
        this.fileId = fileId;
        chunksToRemove = ServerlessDistributedBackupService.systemData.removeOwnerBackedUpChunksOfFileGivingConcurrentList(fileId);
    }

    @Override
    public void run() {
        try {
            ServerlessDistributedBackupService.fileIdToDeleteFileEnhanced.put(fileId, this);
            String deleteMessage = "DELETE " + ServerlessDistributedBackupService.version + " " +
                    ServerlessDistributedBackupService.serverId + " " + fileId + " \r\n\r\n";
            while (chunksToRemove.size() != 0) {
                CommonUtils.sendMulticastMessageMultipleTimesInASeparateThread(deleteMessage, ServerlessDistributedBackupService.MC_Address,
                        ServerlessDistributedBackupService.MC_Port, 3);

                int oneMinuteInMiliseconds = 60000;
                synchronized (mutex) {
                    mutex.wait(oneMinuteInMiliseconds);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //remove this chunks from system data
        ServerlessDistributedBackupService.systemData.removeOwnerBackedUpChunksOfFile(fileId);
        ServerlessDistributedBackupService.systemData.removeOwnerBackedUpFilePathsToFileIdByFileId(fileId);
        ServerlessDistributedBackupService.fileIdToDeleteFileEnhanced.remove(fileId, this);
    }

    public synchronized void ackForDeleteArrived(String chunkId, String senderId) {
        Chunk chunk = decrementReplicationDegreeByChunkId(chunkId, senderId);
        if (chunk == null)
            return;

        if (chunk.getCurrReplicationDegree() <= 0)
            chunksToRemove.remove(chunk);

        if (chunksToRemove.size() == 0) {
            synchronized (mutex) {
                mutex.notify();
            }
        }
    }

    private Chunk decrementReplicationDegreeByChunkId(String chunkId, String senderId) {
        for (Chunk chunk : chunksToRemove)
            if (chunk.getChunkId().equals(chunkId))
                if (chunk.getBackupPeers().contains(senderId)) {
                    chunk.getBackupPeers().remove(senderId);
                    chunk.decrementCurrReplicationDegree();
                    return chunk;
                }

        return null;
    }

}
