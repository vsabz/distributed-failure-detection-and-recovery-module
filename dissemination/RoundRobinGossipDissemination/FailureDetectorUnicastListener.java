package siemens.db.adapt.FailureDetection.dissemination.RoundRobinGossipDissemination;

import siemens.db.adapt.protocol.Failuredetector;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class FailureDetectorUnicastListener extends Thread {

    protected DatagramSocket socket = null;
    protected boolean running = true;
    private RRGossipDissemination rrGossipDissemination;

    public FailureDetectorUnicastListener(RRGossipDissemination rrGossipDissemination) {
        super();
        this.rrGossipDissemination = rrGossipDissemination;
    }

    /**
     * Stops listening hence terminating the thread.
     *
     * @return boolean
     */
    public boolean stopListening() {
        this.running = false;

        return true;
    }


    public void run() {


        try {
            socket = new DatagramSocket(rrGossipDissemination.getFailureDetectorListeningPort());
        } catch (SocketException e) {
            e.printStackTrace();
        }

        while (running) {
            try {
                byte[] buf = new byte[65000];

                // receive request
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                byte[] packetData = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), 0, packetData, 0, packet.getLength());
                Failuredetector.FailureCheck parsedMessage = Failuredetector.FailureCheck.parseFrom(packetData);
                rrGossipDissemination.handleReceivedMessage(parsedMessage);


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        socket.close();
    }
}
