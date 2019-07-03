package ProtocolsENH;

import Protocols.Chunk;
import Protocols.RequestHandlers.DeleteFileChunksRequestHandler;
import Protocols.ServerlessDistributedBackupService;
import Utils.CommonUtils;

import java.util.List;

public class DeleteFileChunksRequestHandlerEnhanced extends DeleteFileChunksRequestHandler {
    public DeleteFileChunksRequestHandlerEnhanced(String fileId) {
        super(fileId);
    }

    public void deleteChunksOfAFile() {
        //removes entries from list of stored chunks and returns the removed chunks
        //(does not try to remove chunks of himself because they will never be in this List)
        List<Chunk> chunksOfFile = ServerlessDistributedBackupService.systemData.removeAllChunksFromCommunityBackedUpChunksByFileId(fileId);
        for (int i = 0; i < chunksOfFile.size(); i++) {
            deleteChunk(chunksOfFile.get(i).getChunkId());
            sendDeleteAck(chunksOfFile.get(i).getNumber());
        }
    }

    private void sendDeleteAck(String chunkNumber) {
        String deleteACK = "DELETEACK " + ServerlessDistributedBackupService.version + " " + ServerlessDistributedBackupService.serverId
                + " " + fileId + " " + chunkNumber + " \r\n\r\n";

        CommonUtils.sendMulticastMessageMultipleTimesInASeparateThread(deleteACK, ServerlessDistributedBackupService.MC_Address,
                ServerlessDistributedBackupService.MC_Port, 3);
    }

}
