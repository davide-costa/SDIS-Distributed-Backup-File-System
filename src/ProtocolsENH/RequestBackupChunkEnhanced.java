package ProtocolsENH;

import Protocols.Chunk;
import Protocols.Requesters.RequestBackupChunk;
import Protocols.ServerlessDistributedBackupService;
import Utils.CommonUtils;


public class RequestBackupChunkEnhanced extends RequestBackupChunk {
    public RequestBackupChunkEnhanced(Chunk chunk, byte[] chunkData) {
        super(chunk, chunkData);
    }

    public synchronized void storedNotificationArrived(String backupPeerId) {
        if (chunk.getCurrReplicationDegree() >= chunk.getExpectedReplicationDegree())
            return;

        if (!chunk.alreadyContainsBackupPeer(backupPeerId)) {
            chunk.addBackupPeers(backupPeerId);
            chunk.incrementCurrReplicationDegree();

            byte[] permissionGrantedMessage = buildPermissionGrantedMessage(backupPeerId);
            CommonUtils.sendMulticastMessageMultipleTimesInASeparateThread(new String(permissionGrantedMessage), ServerlessDistributedBackupService.MC_Address,
                    ServerlessDistributedBackupService.MC_Port, 3, 200);
        }

        terminateIfReplicationDegreeIsSatisfied();
    }

    private byte[] buildPermissionGrantedMessage(String backupPeerId) {
        byte[] messageHeader = ("CANSTORE " + ServerlessDistributedBackupService.version + " " + ServerlessDistributedBackupService.serverId +
                " " + chunk.getFileId() + " " + chunk.getNumber() + " " + backupPeerId + " \r\n\r\n").getBytes();

        return messageHeader;
    }

}
