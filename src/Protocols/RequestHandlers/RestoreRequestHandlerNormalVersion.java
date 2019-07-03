package Protocols.RequestHandlers;


import Protocols.ServerlessDistributedBackupService;
import UDPMessengers.UDPMulticastMessenger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ThreadLocalRandom;

public class RestoreRequestHandlerNormalVersion extends RestoreRequestHandler {
    String senderId;
    public final Object mutex = new Object();
    private final int minDelayToSendChunk = 0;
    private final int maxDelayToSendChunk = 400;
    private boolean cancelReply = false;

    public RestoreRequestHandlerNormalVersion(String senderId, String fileId, String chunkNumber) {
        super(fileId, chunkNumber);
        this.senderId = senderId;
    }

    public void cancelRestoreReply() {
        synchronized (mutex) {
            cancelReply = true;
            mutex.notify();
        }
    }

    public boolean handleRequest() {
        if (!super.handleRequest())
            return false;

        InetAddress MDR_Adress = ServerlessDistributedBackupService.MDR_Address;
        int MDR_Port = ServerlessDistributedBackupService.MDR_Port;

        //add mapping from chunk id that is being requested to this thread (responsible for handling it), so that when the response arrives, this thread will be notified that it should not respond to it, because another peer has already done so
        ServerlessDistributedBackupService.chunkIdToRestoreRequestHandlerThread.put(chunk.getChunkId(), this);

        try {
            synchronized (mutex) {
                mutex.wait(ThreadLocalRandom.current().nextInt(minDelayToSendChunk, maxDelayToSendChunk + 1));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (cancelReply)
            return false;

        try {
            UDPMulticastMessenger messenger = new UDPMulticastMessenger(MDR_Adress, MDR_Port, false);
            messenger.SendMessage(responseMessage);
        } catch (IOException ex) {
            System.err.println("Error sending chunk to multicast socket.");
        }

        return true;
    }

}