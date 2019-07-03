package RMITestApp;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp {
    private static ServerlessDistributedBackupServiceInterfaceRMI stub;
    private static String[] arguments;

    public static void main(String[] args) {
        arguments = args;
        if (arguments.length < 2) {
            System.out.println("Usage: TestApp <peer_ap> <sub_protocol> <opnd_1> <opnd_2>");
            return;
        }

        try {
            // <peer_ap> on format //host/name
            String peerAcessPoint = arguments[0];
            int hostNameSlash = peerAcessPoint.lastIndexOf("/");
            if (hostNameSlash == -1) {
                System.out.println("<peer_ap> on format //host/name");
                return;
            }
            String host = peerAcessPoint.substring(2, hostNameSlash);
            String name = peerAcessPoint.substring(hostNameSlash + 1);

            Registry registry = LocateRegistry.getRegistry(host);
            stub = (ServerlessDistributedBackupServiceInterfaceRMI) registry.lookup(name);

            String operation = TestApp.arguments[1];
            switch (operation) {
                case "BACKUP":
                    backup(false);
                    break;
                case "BACKUPENH":
                    backup(true);
                    break;
                case "RESTORE":
                    restore(false);
                    break;
                case "RESTOREENH":
                    restore(true);
                    break;
                case "DELETE":
                    delete(false);
                    break;
                case "DELETEENH":
                    delete(true);
                    break;
                case "RECLAIM":
                    reclaimProtocol();
                    break;
                case "STATE":
                    stateProtocol();
                    break;
                default:
                    System.err.println("Unrecognized protocol. Supported protocols are: BACKUP, RESTORE, DELETE, RECLAIM, STATE");
                    System.exit(-1);
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error in RMI call!");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void backup(boolean isEnhanced) {
        if (arguments.length < 4) {
            String messageError = "Usage: TestApp <peer_ap> BACKUP";
            if (isEnhanced)
                messageError += "ENH";
            messageError += " <filepath> <replication_degree>";
            System.err.println(messageError);
            System.exit(-1);
        }

        String filepath = arguments[2];
        int replicationDegree = Integer.parseInt(arguments[3]);
        if (replicationDegree > 9) {
            System.err.println("Replication degree must be 9 ou lower!");
            System.exit(-1);
        }

        String response;
        if (isEnhanced)
            response = backupProtocolENH(filepath, replicationDegree);
        else
            response = backupProtocol(filepath, replicationDegree);
        System.out.println(response);
    }

    private static String backupProtocol(String filepath, int replicationDegree) {
        try {
            return stub.backup(filepath, replicationDegree);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static String backupProtocolENH(String filepath, int replicationDegree) {
        try {
            return stub.backupENH(filepath, replicationDegree);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void restore(boolean isEnhanced) {
        if (arguments.length < 3) {
            System.err.println("Usage: TestApp <peer_ap> RESTORE <filepath>");
            System.exit(-1);
        }

        String filepath = arguments[2];
        try {
            if (isEnhanced)
                restoreProtocolENH(filepath);
            else
                restoreProtocol(filepath);
            stub.restore(filepath);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void restoreProtocol(String filepath) {
        try {
            stub.restore(filepath);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void restoreProtocolENH(String filepath) {
        try {
            stub.restoreENH(filepath);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void delete(boolean isEnhanced) {
        if (arguments.length < 3) {
            System.err.println("Usage: TestApp <peer_ap> DELETE <filepath>");
            System.exit(-1);
        }

        String filepath = arguments[2];
        if (isEnhanced)
            deleteENHProtocol(filepath);
        else
            deleteProtocol(filepath);
    }

    private static void deleteProtocol(String filepath) {
        try {
            stub.delete(filepath);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void deleteENHProtocol(String filepath) {
        try {
            stub.deleteENH(filepath);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void reclaimProtocol() {
        if (arguments.length < 3) {
            System.err.println("Usage: TestApp <peer_ap> RESTORE <max_space_to_chunks_in_Kbytes>");
            System.exit(-1);
        }

        int maxSpaceToChunksInKBytes = Integer.parseInt(arguments[2]);
        try {
            stub.reclaim(maxSpaceToChunksInKBytes);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void stateProtocol() {
        try {
            System.out.println(stub.state());
        } catch (RemoteException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}