package Protocols.RequestHandlers;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;

import java.io.File;
import java.util.List;


public class DeleteFileChunksRequestHandler {
    protected String fileId;

    public DeleteFileChunksRequestHandler(String fileId) {
        this.fileId = fileId;
    }

    public void deleteChunksOfAFile() {
        //removes entries from list of stored chunks and returns the removed chunks
        //(does not try to remove chunks of himself because they will never be in this List)
        List<Chunk> chunksOfFile = ServerlessDistributedBackupService.systemData.removeAllChunksFromCommunityBackedUpChunksByFileId(fileId);
        for (int i = 0; i < chunksOfFile.size(); i++)
            deleteChunk(chunksOfFile.get(i).getChunkId());
    }

    protected void deleteChunk(String chunkFilename) {
        // remove chunk file from system
        String filepath = ServerlessDistributedBackupService.chunksPath + chunkFilename;
        File file = new File(filepath);
        if (!file.delete())
            System.err.println("Chunk to delete not found in system!");
    }

}
