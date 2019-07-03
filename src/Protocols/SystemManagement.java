package Protocols;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SystemManagement implements Serializable {
    private static final long serialVersionUID = 4274720516046778792L;
    private static final int GIGABYTE = 1024 * 1024 * 1024;

    private ConcurrentHashMap<String, String> ownerBackedUpFilePathsToFileId = new ConcurrentHashMap<String, String>();
    private List<Chunk> ownerBackedUpChunks = Collections.synchronizedList(new ArrayList<Chunk>());
    private List<Chunk> communityBackedUpChunks = Collections.synchronizedList(new ArrayList<Chunk>());
    private AtomicLong currBackedUpSpace = new AtomicLong(0);
    private AtomicLong currCommunityBackedUpSpace = new AtomicLong(0);
    private AtomicLong maxAmountDiskSpaceForBackup = new AtomicLong(5 * GIGABYTE);


    public ConcurrentHashMap<String, String> getOwnerBackedUpFilePathsToFileId() {
        return ownerBackedUpFilePathsToFileId;
    }

    public String getOwnerBackedUpFileIdByFilePath(String filepath) {
        return ownerBackedUpFilePathsToFileId.get(filepath);
    }

    public List<Chunk> getOwnerBackedUpChunks() {
        return ownerBackedUpChunks;
    }

    public List<Chunk> getOwnerBackedUpChunksOfFile(String fileId) {
        List<Chunk> fileChunks = Collections.synchronizedList(new ArrayList<Chunk>());
        for (int i = 0; i < this.ownerBackedUpChunks.size(); i++)
            if (this.ownerBackedUpChunks.get(i).getFileId().equals(fileId))
                fileChunks.add(this.ownerBackedUpChunks.get(i));

        return fileChunks;
    }

    public void setOwnerBackedUpChunks(ArrayList<Chunk> ownerBackedUpChunks) {
        this.ownerBackedUpChunks = ownerBackedUpChunks;

        //reset currBackedUpSpace
        currBackedUpSpace.set(0);
        for (int i = 0; i < ownerBackedUpChunks.size(); i++)
            currBackedUpSpace.getAndAdd(ownerBackedUpChunks.get(i).getChunkSize());
    }

    public ArrayList<Chunk> removeOwnerBackedUpChunksOfFile(String fileId) {
        ArrayList<Chunk> chunksToRemove = new ArrayList<Chunk>();
        for (Chunk chunk : ownerBackedUpChunks) {
            if (chunk.getFileId().equals(fileId)) {
                chunksToRemove.add(chunk);
                this.currBackedUpSpace.getAndAdd(-chunk.getChunkSize());
            }
        }
        ownerBackedUpChunks.removeAll(chunksToRemove);

        if (chunksToRemove.size() == 0)
            return null;

        return chunksToRemove;
    }

    public List<Chunk> removeOwnerBackedUpChunksOfFileGivingConcurrentList(String fileId) {
        List<Chunk> chunksToRemove = Collections.synchronizedList(new ArrayList<Chunk>());
        for (Chunk chunk : ownerBackedUpChunks) {
            if (chunk.getFileId().equals(fileId)) {
                chunksToRemove.add(chunk);
                this.currBackedUpSpace.getAndAdd(-chunk.getChunkSize());
            }
        }
        ownerBackedUpChunks.removeAll(chunksToRemove);

        if (chunksToRemove.size() == 0)
            return null;

        return chunksToRemove;
    }

    public void removeOwnerBackedUpFilePathsToFileIdByFileId(String fileId) {
        this.ownerBackedUpFilePathsToFileId.values().remove(fileId);
    }

    public void addToOwnerBackedUpFilePaths(String filePath, String fileId) {
        this.ownerBackedUpFilePathsToFileId.put(filePath, fileId);
    }

    public void addToOwnerBackedUpChunks(Chunk ownerBackedUpChunk) {
        this.ownerBackedUpChunks.add(ownerBackedUpChunk);
        this.currBackedUpSpace.getAndAdd(ownerBackedUpChunk.getChunkSize());
    }

    public void setOwnerBackedUpFilePaths(ConcurrentHashMap<String, String> ownerBackedUpFilePathsToFileId) {
        this.ownerBackedUpFilePathsToFileId = ownerBackedUpFilePathsToFileId;
    }

    public List<Chunk> getCommunityBackedUpChunks() {
        return communityBackedUpChunks;
    }

    public Chunk getCommunityBackedUpChunkById(String chunkId) {
        for (int i = 0; i < this.communityBackedUpChunks.size(); i++)
            if (this.communityBackedUpChunks.get(i).getChunkId().equals(chunkId))
                return this.communityBackedUpChunks.get(i);

        return null;
    }

    public List<Chunk> getAllCommunityBackedUpChunksOfAFile(String fileId) {
        List<Chunk> chunksOfAFile = Collections.synchronizedList(new ArrayList<Chunk>());
        for (int i = 0; i < this.communityBackedUpChunks.size(); i++)
            if (this.communityBackedUpChunks.get(i).getFileId().equals(fileId))
                chunksOfAFile.add(this.communityBackedUpChunks.get(i));

        return chunksOfAFile;
    }

    public void setCommunityBackedUpChunks(ArrayList<Chunk> communityBackedUpChunks) {
        this.communityBackedUpChunks = communityBackedUpChunks;

        //reset currCommunityBackedUpSpace
        currCommunityBackedUpSpace = new AtomicLong(0);
        for (int i = 0; i < communityBackedUpChunks.size(); i++)
            currCommunityBackedUpSpace.getAndAdd(communityBackedUpChunks.get(i).getChunkSize());
    }

    public void addToCommunityBackedUpChunks(Chunk chunk) {
        this.communityBackedUpChunks.add(chunk);
        this.incrementCurrCummunityBackedUpSpace(chunk.getChunkSize());
    }

    public void removeChunkFromCommunityBackedUpChunks(Chunk chunk) {
        this.communityBackedUpChunks.remove(chunk);
        this.decrementCurrCummunityBackedUpSpace(chunk.getChunkSize());
    }

    public void removeChunkFromCommunityBackedUpChunksByIndex(int chunkIndex) {
        Chunk chunkRemoved = this.communityBackedUpChunks.remove(chunkIndex);
        this.decrementCurrCummunityBackedUpSpace(chunkRemoved.getChunkSize());
    }

    public void removeChunkFromCommunityBackedUpChunksByIterator(Iterator<Chunk> it, Chunk chunk) {
        this.decrementCurrCummunityBackedUpSpace(chunk.getChunkSize());
        it.remove();
    }

    public List<Chunk> removeAllChunksFromCommunityBackedUpChunksByFileId(String fileId) {
        List<Chunk> chunksOfAFile = Collections.synchronizedList(new ArrayList<Chunk>());
        for (Chunk chunk : this.communityBackedUpChunks) {
            if (chunk.getFileId().equals(fileId)) {
                this.decrementCurrCummunityBackedUpSpace(chunk.getChunkSize());
                chunksOfAFile.add(chunk);
            }
        }
        this.communityBackedUpChunks.removeAll(chunksOfAFile);

        return chunksOfAFile;
    }

    public long getMaxAmountDiskSpaceForBackup() {
        return maxAmountDiskSpaceForBackup.get();
    }

    public void setMaxAmountDiskSpaceForBackup(long maxAmountDiskSpaceForBackup) {
        this.maxAmountDiskSpaceForBackup = new AtomicLong(maxAmountDiskSpaceForBackup);
    }

    public long getCurrBackedUpSpace() {
        return currBackedUpSpace.get();
    }

    public void setCurrBackedUpSpace(long currBackedUpSpace) {
        this.currBackedUpSpace.set(currBackedUpSpace);
    }

    public void incrementCurrBackedUpSpace(long increment) {
        this.currBackedUpSpace.getAndAdd(increment);
    }

    public void decrementCurrBackedUpSpace(long decrement) {
        this.currBackedUpSpace.getAndAdd(-decrement);
    }

    public long getCurrCommunityBackedUpSpace() {
        return currCommunityBackedUpSpace.get();
    }

    public void setCurrCommunityBackedUpSpace(long currCommunityBackedUpSpace) {
        this.currCommunityBackedUpSpace.set(currCommunityBackedUpSpace);
    }

    public void incrementCurrCummunityBackedUpSpace(long increment) {
        this.currCommunityBackedUpSpace.getAndAdd(increment);
    }

    public void decrementCurrCummunityBackedUpSpace(long decrement) {
        this.currCommunityBackedUpSpace.getAndAdd(-decrement);
    }

    public long getAvailableSpaceForBackup() {
        return maxAmountDiskSpaceForBackup.get() - currCommunityBackedUpSpace.get();
    }

    public long getSystemOccupationPercentage() {
        return currCommunityBackedUpSpace.get() * 100 / maxAmountDiskSpaceForBackup.get();
    }

    private void saveSystemInfo() {
        try {
            File systemDataFile = new File(ServerlessDistributedBackupService.systemDataFilePath);
            if (!systemDataFile.exists())
                systemDataFile.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(systemDataFile, false);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public String getSystemData() {
        String systemData = "System data:\n\n";

        //peer info
        systemData += "\tCurrently you have backed up " + ownerBackedUpChunks.size() + " chunks in the community.\n";
        systemData += "\tTotal of " + currBackedUpSpace.get() / 1000 + " Kbytes backed up.\n";

        systemData += "\n\n";

        //community info
        systemData += "\tCurrently are stored " + communityBackedUpChunks.size() + " chunks.\n";
        systemData += "\tTotal of " + currCommunityBackedUpSpace.get() / 1000 + " Kbytes stored.\n";
        systemData += "\tAt " + getSystemOccupationPercentage() + "% capacity.";

        return systemData;
    }

    public ScheduledExecutorService initiatePeriodicSaver(int period) {
        ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable retryTask = new Runnable() {
            @Override
            public void run() {
                SystemManagement.this.saveSystemInfo();
            }
        };
        retryScheduler.scheduleAtFixedRate(retryTask, period, period, TimeUnit.SECONDS);

        return retryScheduler;
    }

}
