package Protocols.Requesters;

import Protocols.Chunk;
import Protocols.ServerlessDistributedBackupService;
import UDPMessengers.UDPMulticastMessenger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestBackupChunk implements Runnable {
    protected Chunk chunk;
    private byte[] chunkData;
    private int delayInSeconds = 1;
    private final int maxRetries = 5;
    protected UDPMulticastMessenger multicastSocket;
    private byte[] requestBackupMessage;
    protected final Object mutex = new Object();
    protected Thread retrySchedulerThread;


    public RequestBackupChunk(Chunk chunk, byte[] chunkData) {
        this.chunk = chunk;
        this.chunkData = chunkData;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setChunk(Chunk chunk) {
        this.chunk = chunk;
    }


    public byte[] getChunkData() {
        return chunkData;
    }

    public void setChunkData(byte[] chunkData) {
        this.chunkData = chunkData;
    }

    @Override
    public void run() {
        try {
            multicastSocket = new UDPMulticastMessenger(ServerlessDistributedBackupService.MDB_Address, ServerlessDistributedBackupService.MDB_Port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        retrySchedulerThread = createRetryTask();    //launch timer for retries


        //add entry in map that maps from chunkId(fileId-ChunkNumber) to a queue of this(the instance of RequstBackupChunk responsable for backup this chunk)
        BlockingQueue<RequestBackupChunk> availableInstances = new LinkedBlockingQueue<RequestBackupChunk>();
        for (int i = 0; i < chunk.getExpectedReplicationDegree(); i++) {
            try {
                availableInstances.put(this);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ServerlessDistributedBackupService.chunkIdToRequestBackupThread.put(chunk.getChunkId(), availableInstances);

        //send chunk
        buildRequestBackupMessage();
        sendChunk();

        long maxTimeToWait = (long) (Math.pow(2, maxRetries) - 1);
        synchronized (mutex) {
            try {
                mutex.wait(maxTimeToWait * 1000);
            } catch (InterruptedException e) {
                System.out.println("Terminating Thread. The chunk has been backed up or the time for backup chunk has over.");
            }
        }

        storeInformationAboutPeersWhoStoredTheChunk();
        retrySchedulerThread.interrupt();
    }

    public synchronized void storedNotificationArrived(String backupPeerId) {
        if (!chunk.alreadyContainsBackupPeer(backupPeerId)) {
            chunk.addBackupPeers(backupPeerId);
            chunk.incrementCurrReplicationDegree();
        }

        terminateIfReplicationDegreeIsSatisfied();
    }

    protected void terminateIfReplicationDegreeIsSatisfied() {
        if (chunk.getCurrReplicationDegree() >= chunk.getExpectedReplicationDegree()) {
            //if desired replication degree is satisfied, shutdown retry Scheduler and thread responsable for this chunk backup
            retrySchedulerThread.interrupt();
            synchronized (mutex) {
                mutex.notify();
            }
        }
    }

    private void storeInformationAboutPeersWhoStoredTheChunk() {
        if (chunk.getCurrReplicationDegree() > 0) {
            ServerlessDistributedBackupService.systemData.addToOwnerBackedUpChunks(chunk);
            ServerlessDistributedBackupService.systemData.incrementCurrBackedUpSpace(chunkData.length);
        } else
            ServerlessDistributedBackupService.systemData.removeOwnerBackedUpFilePathsToFileIdByFileId(chunk.getFileId());

        ServerlessDistributedBackupService.chunkIdToRequestBackupThread.remove(chunk.getChunkId());
    }

    private void sendChunk() {
        try {
            multicastSocket.SendMessage(requestBackupMessage);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private Thread createRetryTask() {
        Thread retrySchedulerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(delayInSeconds * 1000);
                        sendChunk();
                        System.out.println("Chunk " + chunk.getNumber() + " has not been backed up with desired replication degree. Repeating!");
                        delayInSeconds *= 2;
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        retrySchedulerThread.start();

        return retrySchedulerThread;
    }

    private void buildRequestBackupMessage() {
        byte[] messageHeader = ("PUTCHUNK " + ServerlessDistributedBackupService.version + " " + chunk.getSenderId() +
                " " + chunk.getFileId() + " " + chunk.getNumber() + " " + chunk.getExpectedReplicationDegree() + " \r\n\r\n").getBytes();

        requestBackupMessage = new byte[messageHeader.length + chunkData.length];
        System.arraycopy(messageHeader, 0, requestBackupMessage, 0, messageHeader.length);
        System.arraycopy(chunkData, 0, requestBackupMessage, messageHeader.length, chunkData.length);
    }


}
