package Protocols.RequestHandlers;


import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public abstract class RestoreRequestHandler {
    protected String fileId;
    protected String chunkNumber;
    protected String chunkId;
    protected Chunk chunk;
    protected byte[] chunkData;
    protected byte[] responseMessage;

    public RestoreRequestHandler(String fileId, String chunkNumber) {
        this.fileId = fileId;
        this.chunkNumber = chunkNumber;
        chunkId = fileId + "-" + chunkNumber;
    }

    public boolean handleRequest() {
        chunk = ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunkById(chunkId);
        if (chunk == null)
            return false;

        String chunkPath = ServerlessDistributedBackupService.chunksPath + chunkId;
        chunkData = new byte[64 * 1000];
        try {
            FileInputStream inputStream = new FileInputStream(chunkPath);
            int readSize = inputStream.read(chunkData);
            if (readSize < 64000) {
                byte[] smallerData = Arrays.copyOfRange(chunkData, 0, readSize);
                chunkData = smallerData;
            }
            inputStream.close();
        } catch (FileNotFoundException ex) {
            System.err.println("Unable to open file '");
            return false;
        } catch (IOException ex) {
            System.err.println("Error reading file '");
            return false;
        }

        buildRestoreResponseMessage();
        return true;
    }

    private void buildRestoreResponseMessage() {
        byte[] messageHeader = ("CHUNK " + ServerlessDistributedBackupService.version + " " + ServerlessDistributedBackupService.serverId +
                " " + fileId + " " + chunkNumber + "\r\n\r\n").getBytes();

        responseMessage = new byte[messageHeader.length + chunkData.length];
        System.arraycopy(messageHeader, 0, responseMessage, 0, messageHeader.length);
        System.arraycopy(chunkData, 0, responseMessage, messageHeader.length, chunkData.length);
    }

}