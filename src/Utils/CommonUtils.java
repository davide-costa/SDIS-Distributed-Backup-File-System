package Utils;

import UDPMessengers.UDPMulticastMessenger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class CommonUtils {
    public static void sendMulticastMessageMultipleTimes(String message, InetAddress multicastAddress, int multicastPort, int numberRepeats) {
        try {
            UDPMulticastMessenger multicastSocket = new UDPMulticastMessenger(multicastAddress, multicastPort);
            multicastSocket.SendMessage(message.getBytes());
            for (int i = 1; i < numberRepeats; i++) {
                Thread.sleep(i * 1000);
                multicastSocket.SendMessage(message.getBytes());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void sendMulticastMessageMultipleTimesInASeparateThread(String message, InetAddress multicastAddress, int multicastPort, int numberRepeats) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                sendMulticastMessageMultipleTimes(message, multicastAddress, multicastPort, numberRepeats);
            }
        });
        t.start();
    }

    public static void sendMulticastMessageMultipleTimes(byte[] message, UDPMulticastMessenger multicastSocket, int numberRepeats) {
        try {
            multicastSocket.SendMessage(message);
            for (int i = 1; i < numberRepeats; i++) {
                Thread.sleep(i * 1000);
                multicastSocket.SendMessage(message);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitRandomTimeMiliseconds(int minTime, int maxTime) {
        try {
            int randomTime = ThreadLocalRandom.current().nextInt(minTime, maxTime + 1);
            Thread.sleep(randomTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static int generateRandomNumberNotInList(ArrayList<Integer> notNumbers, int min, int max) {
        Integer randomNumber;
        do {
            randomNumber = ThreadLocalRandom.current().nextInt(min, max + 1);
        }
        while (notNumbers.contains(randomNumber));

        return randomNumber;
    }

    public static void sendMulticastMessageMultipleTimes(String message, InetAddress multicastAddress, int multicastPort,
                                                         int numberRepeats, int timeBetweenRepeatsMiliseconds) {
        try {
            UDPMulticastMessenger multicastSocket = new UDPMulticastMessenger(multicastAddress, multicastPort);
            multicastSocket.SendMessage(message.getBytes());
            for (int i = 1; i < numberRepeats; i++) {
                Thread.sleep(timeBetweenRepeatsMiliseconds);
                multicastSocket.SendMessage(message.getBytes());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void sendMulticastMessageMultipleTimesInASeparateThread(String message, InetAddress multicastAddress, int multicastPort,
                                                                          int numberRepeats, int timeBetweenRepeatsMiliseconds) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                sendMulticastMessageMultipleTimes(message, multicastAddress, multicastPort, numberRepeats, timeBetweenRepeatsMiliseconds);
            }
        });
        t.start();
    }
}
