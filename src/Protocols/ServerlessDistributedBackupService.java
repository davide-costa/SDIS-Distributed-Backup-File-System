package Protocols;

import ChannelListenners.ChannelDispatcher;
import ChannelListenners.ChannelListener;
import Protocols.RequestHandlers.BackupChunkRequestHandler;
import Protocols.RequestHandlers.RestoreRequestHandlerNormalVersion;
import Protocols.Requesters.RemoveChunkFromReclaimProtocol;
import Protocols.Requesters.RequestBackupChunk;
import Protocols.Requesters.RequestRestoreChunk;
import ProtocolsENH.RequestBackupChunkEnhanced;
import ProtocolsENH.RequestDeleteFileEnhanced;
import ProtocolsENH.RequestRestoreChunkEnhanced;
import RMITestApp.ServerlessDistributedBackupServiceInterfaceRMI;
import UDPMessengers.UDPMulticastMessenger;
import Utils.CommonUtils;
import Utils.FileUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

public class ServerlessDistributedBackupService implements ServerlessDistributedBackupServiceInterfaceRMI {
    public static ConcurrentHashMap<String, BlockingQueue<RequestBackupChunk>> chunkIdToRequestBackupThread = new ConcurrentHashMap<String, BlockingQueue<RequestBackupChunk>>();
    public static ConcurrentHashMap<String, BackupChunkRequestHandler> chunkIdToBackupChunkRequestHandler = new ConcurrentHashMap<String, BackupChunkRequestHandler>();
    public static ConcurrentHashMap<String, RequestRestoreChunk> chunkIdToRequestRestoreThread = new ConcurrentHashMap<String, RequestRestoreChunk>();
    public static ConcurrentHashMap<String, RestoreRequestHandlerNormalVersion> chunkIdToRestoreRequestHandlerThread = new ConcurrentHashMap<String, RestoreRequestHandlerNormalVersion>();
    public static ConcurrentHashMap<String, ChannelDispatcher> chunkIdToRemoveRequestHandlerThread = new ConcurrentHashMap<String, ChannelDispatcher>();
    public static ConcurrentHashMap<String, RequestDeleteFileEnhanced> fileIdToDeleteFileEnhanced = new ConcurrentHashMap<String, RequestDeleteFileEnhanced>();

    //this set contains the available ports to be listen on for TCP protocol for file restoring protocol
    //thus, the initial capacity of the container indicates the maximum number of threads accepting TCP connections at the same time (but not the amount of restores being made at any given time)
    public static BlockingQueue<Integer> availablePorts = new LinkedBlockingQueue<Integer>();
    public static int firstTCPPort = 5555;
    public static int lastTCPPort = 5755;

    public static SystemManagement systemData;

    public static InetAddress MDB_Address;
    public static int MDB_Port;
    public static InetAddress MDR_Address;
    public static int MDR_Port;
    public static InetAddress MC_Address;
    public static int MC_Port;

    public static String version;
    public static String serverId;

    public static String systemDataFilePath = "ServerlessDistributedBackupService/systemData";
    public static String chunksPath = "ServerlessDistributedBackupService/backedUpChunks/";
    public static String restoredChunksPath = "ServerlessDistributedBackupService/restoredChunks/";
    public static String restoredFilesPath = "ServerlessDistributedBackupService/restoredFiles/";

    public static int numThreadsToBackup = 150;
    public static int numThreadsToRestore = 20;


    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.out.println("Usage: ServerlessDistributedBackupService <version> <serverID> <server_acess_point> " +
                    "<MC_adress:MC_port> <MDB_adress:MDB_port> <MDR_adress:MDR_port>");
            return;
        }

        //parse arguments
        version = args[0];
        serverId = args[1];
        String serverAcessPoint = args[2];
        MC_Address = getAdressFromString(args[3]);
        MC_Port = getPortFromString(args[3]);
        MDB_Address = getAdressFromString(args[4]);
        MDB_Port = getPortFromString(args[4]);
        MDR_Address = getAdressFromString(args[5]);
        MDR_Port = getPortFromString(args[5]);

        //start listening on channels
        startListeningOnChannel(MDB_Address, MDB_Port);
        startListeningOnChannel(MC_Address, MC_Port);
        startListeningOnChannel(MDR_Address, MDR_Port);

        addTCPPortsToAvailablePortsQueue();

        initiateProtocolRMI(serverAcessPoint);

        //create folders if not existents
        createProgramFolders();

        //system data management creation and initiate periodic saver that stores the information at disk at a given period
        if (!readSystemDataIfExists())
            systemData = new SystemManagement();
        systemData.initiatePeriodicSaver(2);
    }

    private static void createProgramFolders() {
        File dir = new File("ServerlessDistributedBackupService");
        dir.mkdir();
        dir = new File("ServerlessDistributedBackupService/backedUpChunks");
        dir.mkdir();
        dir = new File("ServerlessDistributedBackupService/restoredChunks");
        dir.mkdir();
        dir = new File("ServerlessDistributedBackupService/restoredFiles");
        dir.mkdir();
    }

    private static void addTCPPortsToAvailablePortsQueue() {
        try {
            for (int i = firstTCPPort; i <= lastTCPPort; i++)
                availablePorts.put(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void startListeningOnChannel(InetAddress address, int port) throws IOException {
        UDPMulticastMessenger socket = new UDPMulticastMessenger(address, port, true);
        ChannelListener channel = new ChannelListener(socket);
        Thread thread = new Thread(channel);
        thread.start();
    }

    private static boolean readSystemDataIfExists() {
        try {
            FileInputStream fileIn = new FileInputStream(systemDataFilePath);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            systemData = (SystemManagement) in.readObject();
            in.close();
            fileIn.close();
        } catch (ClassNotFoundException | IOException e) {
            return false;
        }

        return true;
    }

    private static void initiateProtocolRMI(String peerAcessPoint) {
        try {
            ServerlessDistributedBackupService serverlessDistributedBackupService = new ServerlessDistributedBackupService();
            ServerlessDistributedBackupServiceInterfaceRMI stub =
                    (ServerlessDistributedBackupServiceInterfaceRMI) UnicastRemoteObject.exportObject(serverlessDistributedBackupService, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(peerAcessPoint, stub);
        } catch (Exception e) {
            System.err.println("ServerlessDistributedBackupService RMI exception: " + e.toString());
            e.printStackTrace();

        }
    }

    @Override
    public String backup(String filepath, int replicationDegree) throws RemoteException {
        return executeBackupBasedOnProgramVersion(0, filepath, replicationDegree);
    }

    @Override
    public String backupENH(String filepath, int replicationDegree) throws RemoteException {
        return executeBackupBasedOnProgramVersion(1, filepath, replicationDegree);
    }


    private String executeBackupBasedOnProgramVersion(int programVersion, String filepath, int replicationDegree) {
        if (ServerlessDistributedBackupService.systemData.getOwnerBackedUpFileIdByFilePath(filepath) != null)
            return "File has already been backed up!";

        try {
            File f = new File(filepath);
            if (!f.exists())
                return "File '" + filepath + "' does not exist!";

            long millionChunksSize = 64 * 1000;
            millionChunksSize *= 1000000;
            if (f.length() > millionChunksSize) //file not big than 1 million chunks (each chunk has 64000 bytes)
                return "File '" + filepath + "' too big to backup!";

            String fileId = FileUtils.generateFileId(filepath);
            String senderId = ServerlessDistributedBackupService.serverId;
            ExecutorService executor = Executors.newFixedThreadPool(numThreadsToBackup);
            byte[] buffer = new byte[64 * 1000]; //64000 bytes for data, in order to the data+header make 64Kb or less
            FileInputStream inputStream = new FileInputStream(filepath);
            int chunkNumber = 0;
            int bytesRead;

            boolean endOfFile = false;
            while (!endOfFile) {
                bytesRead = inputStream.read(buffer);

                if (bytesRead != 64000) {
                    if (bytesRead == -1)
                        bytesRead = 0;

                    byte[] dataRead = Arrays.copyOfRange(buffer, 0, bytesRead);
                    buffer = dataRead;
                    endOfFile = true;
                }

                Chunk chunk = new Chunk(String.valueOf(chunkNumber), fileId, senderId, bytesRead, replicationDegree);

                RequestBackupChunk handleBackupChunk = getBackupChunkClass(programVersion, buffer, chunk);
                executor.execute(handleBackupChunk);
                chunkNumber++;
                buffer = new byte[64 * 1000];
            }

            systemData.addToOwnerBackedUpFilePaths(filepath, fileId);
            inputStream.close();
            executor.shutdown(); // terminates the executor after all running threads
        } catch (FileNotFoundException ex) {
            System.err.println("Unable to open file '" + filepath + "'");
        } catch (IOException ex) {
            System.err.println("Error reading file '" + filepath + "'");
        }

        return "File is being backed up!";
    }

    private RequestBackupChunk getBackupChunkClass(int callId, byte[] buffer, Chunk chunk) {
        if (callId == 0)
            return new RequestBackupChunk(chunk, buffer);
        else
            return new RequestBackupChunkEnhanced(chunk, buffer);
    }

    @Override
    public void restore(String filepath) throws RemoteException {
        String fileId = ServerlessDistributedBackupService.systemData.getOwnerBackedUpFilePathsToFileId().get(filepath);
        List<Chunk> chunks = ServerlessDistributedBackupService.systemData.getOwnerBackedUpChunksOfFile(fileId);
        if (chunks.size() == 0)
            return;

        ExecutorService executor = Executors.newFixedThreadPool(numThreadsToRestore);
        for (Chunk chunk : chunks) {
            RequestRestoreChunk requestRestoreChunk = new RequestRestoreChunk(chunk);
            executor.execute(requestRestoreChunk);
        }
        executor.shutdown(); // terminates the executor after all running tasks

        if (timeoutInCaseChunksNeverComes(executor, chunks))
            return;

        getReceivedChunksAndReassembleToOriginalFile(chunks);
    }

    private boolean timeoutInCaseChunksNeverComes(ExecutorService executor, List<Chunk> chunks) {
        int minBetweenNumThreadsAndChunksSize;
        if (chunks.size() > numThreadsToRestore)
            minBetweenNumThreadsAndChunksSize = chunks.size();
        else
            minBetweenNumThreadsAndChunksSize = numThreadsToRestore;
        int maxTimeToWaitMiliseconds = chunks.size() * 5000 / minBetweenNumThreadsAndChunksSize;

        boolean timeout;
        try {
            timeout = !executor.awaitTermination(maxTimeToWaitMiliseconds, TimeUnit.MILLISECONDS);
            executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            e.printStackTrace();
            return true;
        }

        return timeout;
    }

    @Override
    public void restoreENH(String filepath) throws RemoteException {
        String fileId = ServerlessDistributedBackupService.systemData.getOwnerBackedUpFilePathsToFileId().get(filepath);
        List<Chunk> chunks = ServerlessDistributedBackupService.systemData.getOwnerBackedUpChunksOfFile(fileId);
        if (chunks.size() == 0)
            return;

        ExecutorService executor = Executors.newFixedThreadPool(numThreadsToRestore);
        for (Chunk chunk : chunks) {
            RequestRestoreChunkEnhanced requestRestoreChunk = new RequestRestoreChunkEnhanced(chunk);
            executor.execute(requestRestoreChunk);
        }
        executor.shutdown(); // terminates the executor after all running threads

        timeoutInCaseChunksNeverComes(executor, chunks);

        getReceivedChunksAndReassembleToOriginalFile(chunks);
    }

    private void getReceivedChunksAndReassembleToOriginalFile(List<Chunk> chunks) {
        List<String> chunksPaths = new ArrayList<String>();
        String chunksBasePath = ServerlessDistributedBackupService.restoredChunksPath;
        for (Chunk chunk : chunks) {
            chunksPaths.add(chunksBasePath + chunk.getChunkId());
        }
        String filePath = ServerlessDistributedBackupService.restoredFilesPath + chunks.get(0).getFileId();
        chunksPaths.sort(new Comparator<String>() {
            @Override
            public int compare(String string1, String string2) {
                //chunk number string1
                int dashIndex = string1.indexOf("-");
                int chunkNumberString1 = Integer.parseInt(string1.substring(dashIndex + 1));

                //chunk number string2
                dashIndex = string2.indexOf("-");
                int chunkNumberString2 = Integer.parseInt(string2.substring(dashIndex + 1));

                if (chunkNumberString1 < chunkNumberString2)
                    return -1;
                else if (chunkNumberString1 > chunkNumberString2)
                    return 1;
                else
                    return 0;
            }
        });

        int ret = FileUtils.reassembleChunksToFile(chunksPaths, filePath);
        switch (ret) {
            case 1:
                System.out.println("Error restoring file: at least one of the chunks doesn't exist, thus the file cannot be reassembled.");
                break;
            case 2:
                System.out.println("Error restoring file: exception occurred when reading from chunks or writing to reassembled file.");
                break;
            case 3:
                System.out.println("Warning restoring file: The output stream to the restored file couldn't be closed. This may cause problems editing the file externally. Consider restarting this program to free the file.");
                break;
            default:
                System.out.println("File " + filePath + " restored successfully.");
                break;
        }
    }

    public String getFileIdIfExistent(String filepath) {
        String fileId = systemData.getOwnerBackedUpFileIdByFilePath(filepath);
        if (fileId == null) {
            System.err.println("File " + filepath + " have not been backed up!\n " + "Use \"TestApp <peer_ap> BACKUP "
                    + filepath + " <replication_degree>\" to backup it.");
            return null;
        }

        return fileId;
    }

    @Override
    public void delete(String filepath) throws RemoteException {
        String fileId = getFileIdIfExistent(filepath);
        if (fileId == null)
            return;

        //send delete message to other peers
        String deleteMessage = "DELETE " + ServerlessDistributedBackupService.version + " " +
                ServerlessDistributedBackupService.serverId + " " + fileId + " \r\n\r\n";
        CommonUtils.sendMulticastMessageMultipleTimes(deleteMessage, ServerlessDistributedBackupService.MC_Address,
                ServerlessDistributedBackupService.MC_Port, 3);

        //remove this chunks from system data
        systemData.removeOwnerBackedUpChunksOfFile(fileId);
        systemData.removeOwnerBackedUpFilePathsToFileIdByFileId(fileId);
    }

    @Override
    public void deleteENH(String filepath) {
        String fileId = getFileIdIfExistent(filepath);
        if (fileId == null)
            return;

        //send delete message to other peers and wait for acknowledge
        RequestDeleteFileEnhanced deleteFileEnhanced = new RequestDeleteFileEnhanced(fileId);
        Thread thread = new Thread(deleteFileEnhanced);
        thread.start();
    }

    @Override
    public String state() throws RemoteException {
        return systemData.getSystemData();
    }

    @Override
    public void reclaim(int maxSpaceToChunksInKBytes) throws RemoteException {
        long maxSpaceToChunksBytes = maxSpaceToChunksInKBytes * 1000;
        ServerlessDistributedBackupService.systemData.setMaxAmountDiskSpaceForBackup(maxSpaceToChunksBytes);

        if (ServerlessDistributedBackupService.systemData.getCurrCommunityBackedUpSpace() > maxSpaceToChunksBytes) {
            ArrayList<Chunk> chunksRemoved = removeChunksBasedOnReplicationDegree();
            removeChunksFromDisk(chunksRemoved);
            sendRemoveChunksMessages(chunksRemoved);
        }
    }

    private void removeChunksFromDisk(ArrayList<Chunk> chunksToRemove) {
        for (Chunk chunk : chunksToRemove) {
            String filepath = ServerlessDistributedBackupService.chunksPath + chunk.getChunkId();
            File file = new File(filepath);
            file.delete();
        }
    }

    private ArrayList<Chunk> removeChunksBasedOnReplicationDegree() {
        ArrayList<Chunk> chunksRemoved = new ArrayList<Chunk>();
        List<Chunk> communityStoredChunks = ServerlessDistributedBackupService.systemData.getCommunityBackedUpChunks();
        long maxSpaceToChunks = ServerlessDistributedBackupService.systemData.getMaxAmountDiskSpaceForBackup();

        //first remove chunks that curr replication degree is bigger than requested
        for (Iterator<Chunk> it = communityStoredChunks.iterator(); it.hasNext(); ) {
            if (ServerlessDistributedBackupService.systemData.getCurrCommunityBackedUpSpace() > maxSpaceToChunks) {
                Chunk chunk = it.next();
                if (chunk.getCurrReplicationDegree() > chunk.getExpectedReplicationDegree()) {
                    chunksRemoved.add(chunk);
                    ServerlessDistributedBackupService.systemData.removeChunkFromCommunityBackedUpChunksByIterator(it, chunk);
                }
            } else
                return chunksRemoved;
        }


        //if last step not enough, remove randomly chunks until get in the max occupied space range
        ArrayList<Integer> chunksToRemoveIdx = new ArrayList<Integer>();
        while (ServerlessDistributedBackupService.systemData.getCurrCommunityBackedUpSpace() > maxSpaceToChunks) {
            int randomIndex = ThreadLocalRandom.current().nextInt(0, communityStoredChunks.size());
            chunksRemoved.add(communityStoredChunks.get(randomIndex));
            chunksToRemoveIdx.add(randomIndex);
            ServerlessDistributedBackupService.systemData.removeChunkFromCommunityBackedUpChunksByIndex(randomIndex);
        }

        return chunksRemoved;
    }

    private void sendRemoveChunksMessages(ArrayList<Chunk> chunksRemoved) {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (int i = 0; i < chunksRemoved.size(); i++) {
            Chunk chunk = chunksRemoved.get(i);
            RemoveChunkFromReclaimProtocol removeChunkFromReclaimProtocol = new RemoveChunkFromReclaimProtocol(chunk);
            executor.execute(removeChunkFromReclaimProtocol);
        }

        executor.shutdown(); // terminates the executor after all running threads
    }

    private static InetAddress getAdressFromString(String string) {
        int indexOfDoblePoints = string.indexOf(":");
        try {
            return InetAddress.getByName(string.substring(0, indexOfDoblePoints));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static int getPortFromString(String string) {
        int indexOfDoblePoints = string.indexOf(":");
        return Integer.parseInt(string.substring(indexOfDoblePoints + 1));
    }

}
