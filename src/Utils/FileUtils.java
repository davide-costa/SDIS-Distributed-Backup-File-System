package Utils;


import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class FileUtils {
    public static String generateFileId(String filepathStr) {
        String fileIdHash = null;
        try {
            Path filepath = Paths.get(filepathStr);
            long filesize = Files.size(filepath);
            long lastModifiedTime = Files.getLastModifiedTime(filepath).toMillis();
            String ownerName = Files.getOwner(filepath).getName();

            String fileInfo = "" + filepath + filesize + lastModifiedTime + ownerName;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            fileIdHash = (new HexBinaryAdapter()).marshal(digest.digest(fileInfo.getBytes())).toLowerCase();
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return fileIdHash;
    }

    //Returns 0 on success and if all files have been assembled successfully; 1 if file doesn't exist; 2 on reading exception; 3 if couldn't close the the output file (not big problem, just a warning)
    public static int reassembleChunksToFile(List<String> chunksPaths, String filePath) {
        if (chunksPaths.size() == 0)
            return 1;

        //open stream to file to store backed up file on
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(filePath);
        } catch (IOException ex) {
            return 2;
        }

        for (String chunkPath : chunksPaths) {
            byte[] data = new byte[64 * 1000];
            try {
                File f = new File(chunkPath);
                if (!f.exists()) {
                    outputStream.close();
                    return 1;
                }
                //Read data from the current chunk
                FileInputStream inputStream = new FileInputStream(chunkPath);
                int readSize = inputStream.read(data);
                if (readSize < 64000) {
                    byte[] smallerData = Arrays.copyOfRange(data, 0, readSize);
                    data = smallerData;
                }
                inputStream.close();

                //write the current chunk data to the file that will contain data from all chunks
                outputStream.write(data);
            } catch (IOException ex) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                deleteFileFromDisk(filePath);
                return 2;
            }
        }

        try {
            outputStream.close();
        } catch (IOException ex) {
            return 3;
        }

        return 0;
    }

    public static boolean deleteFileFromDisk(String filepath) {
        File file = new File(filepath);
        return file.delete();
    }

}
