package ChannelListenners;

import Protocols.Chunk;
import Protocols.RequestHandlers.BackupChunkRequestHandler;
import Protocols.RequestHandlers.DeleteFileChunksRequestHandler;
import Protocols.RequestHandlers.RestoreRequestHandler;
import Protocols.RequestHandlers.RestoreRequestHandlerNormalVersion;
import Protocols.Requesters.RequestBackupChunk;
import Protocols.Requesters.RequestRestoreChunk;
import Protocols.ServerlessDistributedBackupService;
import ProtocolsENH.BackupChunkRequestHandlerEnhanced;
import ProtocolsENH.DeleteFileChunksRequestHandlerEnhanced;
import ProtocolsENH.RequestDeleteFileEnhanced;
import ProtocolsENH.RestoreRequestHandlerEnhancedVersion;
import Utils.ByteUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;


public class ChannelDispatcher implements Runnable {
    private InetAddress sourceIpAdress;
    private byte[] receivedBytes; //contains the bytes corresponding to the received message and the received chunkData (may be applicable or not)
    private String[] receivedMsgArray;
    private byte[] receivedData;
    private final Object mutex = new Object();

    ChannelDispatcher(byte[] received_bytes, InetAddress sourceIpAdress) {
        this.sourceIpAdress = sourceIpAdress;
        this.receivedBytes = received_bytes;
    }

    private boolean parseReceivedBytes() {
        int delimiterPos = ByteUtils.indexOf(receivedBytes, "\r\n\r\n");
        if (delimiterPos == -1)
            return false;

        String receivedMsg = new String(Arrays.copyOfRange(receivedBytes, 0, delimiterPos));
        receivedMsg = receivedMsg.trim().replaceAll(" +", " ");
        receivedMsgArray = receivedMsg.split(" ");
        receivedData = Arrays.copyOfRange(receivedBytes, delimiterPos + 4, receivedBytes.length);

        return true;
    }

    @Override
    public void run() {
        boolean messageCorrectlyParsed = parseReceivedBytes();
        if (!messageCorrectlyParsed)
            return;

        String taskIdentifier = receivedMsgArray[0];
        switch (taskIdentifier) {
            case "PUTCHUNK":
                handlePutChunkTask();
                break;
            case "STORED":
                handleStoredTask();
                break;
            case "GETCHUNK":
                handleGetChunkTask();
                break;
            case "CHUNK":
                handleChunkTask();
                break;
            case "DELETE":
                handleDeleteTask();
                break;
            case "DELETEACK":
                handleDeleteACKTask();
                break;
            case "REMOVED":
                handleRemovedTask();
                break;
            case "CANSTORE":
                handleCanStoreTask();
                break;
            default:
                System.out.println("Received unrecognized task identifier: " + taskIdentifier);
                break;
        }
    }

    private void handlePutChunkTask() {
        String programVersion = receivedMsgArray[1];
        String senderId = receivedMsgArray[2];
        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return;
        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];
        String chunkId = fileId + "-" + chunkNumber;
        int replicationDeg = Integer.parseInt(receivedMsgArray[5]);

        ChannelDispatcher channelDispatcher = ServerlessDistributedBackupService.chunkIdToRemoveRequestHandlerThread.get(chunkId);
        if (channelDispatcher != null) {
            channelDispatcher.notifyPutChunkArrived();
            return;
        }

        BackupChunkRequestHandler handler;
        if (programVersion.equals("1.0"))
            handler = new BackupChunkRequestHandler(senderId, fileId, chunkNumber, replicationDeg, receivedData);
        else if (programVersion.equals("2.0"))
            handler = new BackupChunkRequestHandlerEnhanced(senderId, fileId, chunkNumber, replicationDeg, receivedData);
        else {
            System.out.println("Received unrecognized message");
            return;
        }

        handler.backupChunk();
    }

    private void notifyPutChunkArrived() {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    private void waitForPutChunk(int randomTime) {
        synchronized (mutex) {
            try {
                mutex.wait(randomTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleStoredTask() {
        String senderId = receivedMsgArray[2];
        if (senderId.equals(ServerlessDistributedBackupService.serverId)) //checks if the message not come from itself
            return;

        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];
        String chunkId = fileId + "-" + chunkNumber;
        BlockingQueue<RequestBackupChunk> availableInstances = ServerlessDistributedBackupService.chunkIdToRequestBackupThread.get(chunkId);
        RequestBackupChunk requestBackupChunk = null;
        try {
            if (availableInstances != null) {
                requestBackupChunk = availableInstances.take();
                if (ServerlessDistributedBackupService.version.equals("1.0"))
                    availableInstances.put(requestBackupChunk); //makes sure that in version 1.0 the replication degree can be any number that is superior to requested
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //if expecting a stored message for that chunk
        if (requestBackupChunk != null)
            requestBackupChunk.storedNotificationArrived(senderId);//inform the thread responsible for the chunk backup that a store has arrived
        else
            handleStoredAndCanStoreMessageIfHasChunkStored(chunkId, senderId); //packet not from a file of this peer, but it can be from a stored chunk that this peers haves
    }

    private void handleStoredAndCanStoreMessageIfHasChunkStored(String chunkId, String newBackupPeerId) {
        Chunk chunk = ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunkById(chunkId);
        if (chunk == null) //checks if peer has the chunk stored, based on its chunk id
            return;

        if (chunk.getBackupPeers().contains(newBackupPeerId)) //checks if the peer has the chunk already backed up
            return;

        chunk.incrementCurrReplicationDegree();
        chunk.addBackupPeers(newBackupPeerId);
    }

    private void handleCanStoreTask() {
        String senderId = receivedMsgArray[2];
        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return;

        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];
        String chunkId = fileId + "-" + chunkNumber;
        String peerIdThatCanStoreTheChunk = receivedMsgArray[5];
        if (!peerIdThatCanStoreTheChunk.equals(ServerlessDistributedBackupService.serverId))
            return;

        BackupChunkRequestHandlerEnhanced backupChunkRequestHandler =
                (BackupChunkRequestHandlerEnhanced) ServerlessDistributedBackupService.chunkIdToBackupChunkRequestHandler.remove(chunkId);

        if (backupChunkRequestHandler == null) {
            handleStoredAndCanStoreMessageIfHasChunkStored(chunkId, senderId);
            return;
        }

        //inform the thread responsible for the storing the chunk that a can store message has arrived
        backupChunkRequestHandler.canStoreArrived(senderId, peerIdThatCanStoreTheChunk);
    }

    private void handleGetChunkTask() {
        String version = receivedMsgArray[1];
        String senderId = receivedMsgArray[2];
        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];

        RestoreRequestHandler handler;
        if (version.equals("1.0"))
            handler = new RestoreRequestHandlerNormalVersion(senderId, fileId, chunkNumber);
        else {
            if (receivedMsgArray.length < 7)
                return;
            int destinationPort = Integer.parseInt(receivedMsgArray[6]);
            handler = new RestoreRequestHandlerEnhancedVersion(fileId, chunkNumber, sourceIpAdress.getHostName(), destinationPort);
        }

        handler.handleRequest();
    }

    private void handleChunkTask() {
        String senderId = receivedMsgArray[2];
        String fileIdStr = receivedMsgArray[3];
        String chunkNumberStr = receivedMsgArray[4];
        String ChunkId = fileIdStr + "-" + chunkNumberStr;

        //ensure peer doesn't try to respond to its own CHUNK request
        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return;

        RequestRestoreChunk requestBackupChunk = ServerlessDistributedBackupService.chunkIdToRequestRestoreThread.remove(ChunkId);
        if (requestBackupChunk != null) //was response for a restore request of this peer
        {
            //inform the thread responsible for the chunk backup that a store has arrived
            requestBackupChunk.chunkArrived(senderId, receivedData);
        } else {
            RestoreRequestHandlerNormalVersion restoreRequestHandler = ServerlessDistributedBackupService.chunkIdToRestoreRequestHandlerThread.remove(ChunkId);
            if (restoreRequestHandler != null)
                restoreRequestHandler.cancelRestoreReply();
        }
    }

    private void handleDeleteTask() {
        String version = receivedMsgArray[1];
        String senderId = receivedMsgArray[2];
        String fileId = receivedMsgArray[3];

        if (senderId.equals(ServerlessDistributedBackupService.serverId))
            return;

        DeleteFileChunksRequestHandler deleteFileChunksRequestHandler;
        if (version.equals("1.0"))
            deleteFileChunksRequestHandler = new DeleteFileChunksRequestHandler(fileId);
        else
            deleteFileChunksRequestHandler = new DeleteFileChunksRequestHandlerEnhanced(fileId);

        deleteFileChunksRequestHandler.deleteChunksOfAFile();
    }

    private void handleDeleteACKTask() {
        String senderId = receivedMsgArray[2];
        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];
        String chunkId = fileId + "-" + chunkNumber;

        RequestDeleteFileEnhanced deleteFileEnhanced = ServerlessDistributedBackupService.fileIdToDeleteFileEnhanced.get(fileId);
        if (deleteFileEnhanced == null)
            return;

        deleteFileEnhanced.ackForDeleteArrived(chunkId, senderId);
    }

    private void handleRemovedTask() {
        String senderId = receivedMsgArray[2];
        String fileId = receivedMsgArray[3];
        String chunkNumber = receivedMsgArray[4];
        String chunkId = fileId + "-" + chunkNumber;

        Chunk removedChunk;
        if (!senderId.equals(ServerlessDistributedBackupService.serverId)) //not a message from itself
            if ((removedChunk = ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunkById(chunkId)) != null) //check if this peer has a local copy of the chunk
            {
                removedChunk.decrementCurrReplicationDegree();
                if (removedChunk.isCurrReplicationDegreeLowerThanExpected()) {
                    ServerlessDistributedBackupService.chunkIdToRemoveRequestHandlerThread.put(chunkId, this);
                    int randomTime = ThreadLocalRandom.current().nextInt(0, 400 + 1);
                    waitForPutChunk(randomTime);
                    backupChunk(removedChunk);
                }
            }
    }

    private void backupChunk(Chunk removedChunk) {
        byte[] chunkData = loadChunkFromSystem(removedChunk.getChunkId());
        if (chunkData == null) {
            System.out.println("Error reading file'" + removedChunk.getChunkId() + "' that supposed to be in file system");
            return;
        }

        RequestBackupChunk handleBackupChunk = new RequestBackupChunk(removedChunk, chunkData);
        handleBackupChunk.run();
    }

    private byte[] loadChunkFromSystem(String chunkId) {
        String filepath = ServerlessDistributedBackupService.chunksPath + chunkId;
        File file = new File(filepath);
        if (!file.exists())
            return null;
        byte[] chunkData = new byte[(int) file.length()];

        try {
            FileInputStream fileIn = new FileInputStream(filepath);
            fileIn.read(chunkData);
            fileIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return chunkData;
    }

}
