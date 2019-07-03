package Protocols;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Chunk implements Comparable<Object>, Serializable {
    private static final long serialVersionUID = -1803682345293214003L;
    private String chunkId;
    private String number;
    private String fileId;
    private String senderId;
    private long chunkSize;
    private AtomicInteger expectedReplicationDegree = new AtomicInteger();
    private AtomicInteger currReplicationDegree = new AtomicInteger();
    private List<String> backupPeers = Collections.synchronizedList(new ArrayList<String>());


    public Chunk(String number, String fileId, String senderId, long chunkSize, int expectedReplicationDegree) {
        this.number = number;
        this.fileId = fileId;
        this.senderId = senderId;
        this.chunkSize = chunkSize;
        this.chunkId = fileId + "-" + number;
        this.expectedReplicationDegree.set(expectedReplicationDegree);
    }

    public Chunk(Chunk chunk) {
        this.number = chunk.getNumber();
        this.fileId = chunk.getFileId();
        this.senderId = chunk.getSenderId();
        this.chunkId = chunk.fileId + "-" + chunk.number;
        this.chunkSize = chunk.chunkSize;
        this.expectedReplicationDegree.set(chunk.getExpectedReplicationDegree());
        this.currReplicationDegree.set(chunk.getCurrReplicationDegree());
        this.backupPeers = chunk.getBackupPeers();
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getChunkId() {
        return chunkId;
    }

    public void setChunkId(String filePath) {
        this.chunkId = filePath;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getExpectedReplicationDegree() {
        return expectedReplicationDegree.get();
    }

    public void setExpectedReplicationDegree(int expectedReplicationDegree) {
        this.expectedReplicationDegree.set(expectedReplicationDegree);
    }

    public int getCurrReplicationDegree() {
        return currReplicationDegree.get();
    }

    public void incrementCurrReplicationDegree() {
        this.currReplicationDegree.getAndIncrement();
    }

    public void decrementCurrReplicationDegree() {
        this.currReplicationDegree.getAndDecrement();
    }

    public void setCurrReplicationDegree(int currReplicationDegree) {
        this.currReplicationDegree.set(currReplicationDegree);
    }

    public boolean isCurrReplicationDegreeLowerThanExpected() {
        return currReplicationDegree.get() < expectedReplicationDegree.get();
    }

    public List<String> getBackupPeers() {
        return backupPeers;
    }

    public void setBackupPeers(ArrayList<String> backupPeers) {
        this.backupPeers = backupPeers;
    }

    public void addBackupPeers(String backupPeer) {
        this.backupPeers.add(backupPeer);
    }

    public boolean alreadyContainsBackupPeer(String backupPeerId) {
        return backupPeers.contains(backupPeerId);
    }

    @Override
    public int compareTo(Object object) {
        return chunkId.compareTo(((Chunk) object).chunkId);
    }

}
