package siemens.db.adapt.FailureDetection.dissemination.BestEffordBroadcastDissemination;

import siemens.db.adapt.protocol.Failuredetector;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

/**
 * This class disseminates messages in multicast way.
 */
public class MulticastDisseminationRunnable extends Thread {

    private Failuredetector.FailureCheck.Builder failureCheck;
    protected DatagramSocket socket = null;
    protected BufferedReader in = null;
    private int FDListeningPort;

    MulticastDisseminationRunnable(Failuredetector.FailureCheck.Builder failureCheck, int FDListeningPort) {
        this.failureCheck = failureCheck;
        this.FDListeningPort = FDListeningPort;

        try {

            socket = new DatagramSocket();

        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        Failuredetector.FailureCheck update = failureCheck.build();

        try {
            byte[] buf = new byte[65000];

            buf = update.toByteArray();

            // send
            InetAddress group = InetAddress.getByName("230.0.0.1");
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, FDListeningPort);
            socket.send(packet);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
