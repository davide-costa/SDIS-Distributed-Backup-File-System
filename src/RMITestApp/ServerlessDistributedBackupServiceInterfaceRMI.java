package RMITestApp;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ServerlessDistributedBackupServiceInterfaceRMI extends Remote {
    String backup(String filepath, int replicationDegree) throws RemoteException;

    String backupENH(String filepath, int replicationDegree) throws RemoteException;

    void restore(String filepath) throws RemoteException;

    void restoreENH(String filepath) throws RemoteException;

    void delete(String filepath) throws RemoteException;

    void deleteENH(String filepath) throws RemoteException;

    void reclaim(int maxSpaceToChunksInKBytes) throws RemoteException;

    String state() throws RemoteException;
}