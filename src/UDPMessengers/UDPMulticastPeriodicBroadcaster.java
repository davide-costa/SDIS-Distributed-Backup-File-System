package UDPMessengers;

import java.io.IOException;

public class UDPMulticastPeriodicBroadcaster implements Runnable {
    private UDPMulticastMessenger messenger;
    private String message;
    private int delay;
    private boolean run;

    public UDPMulticastPeriodicBroadcaster(UDPMulticastMessenger messenger, String message, int delay) {
        this.messenger = messenger;
        this.message = message;
        this.delay = delay;
        this.run = true;
    }

    public void stopBroadcasting() {
        this.run = false;
    }

    @Override
    public void run() {
        while (this.run) {
            try {
                messenger.SendMessage(message);
                Thread.sleep(delay);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
