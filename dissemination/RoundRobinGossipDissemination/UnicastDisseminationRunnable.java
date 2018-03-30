package siemens.db.adapt.FailureDetection.dissemination.RoundRobinGossipDissemination;

import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;
import java.io.IOException;
import java.net.*;

public class UnicastDisseminationRunnable implements Runnable {


    PeerReplica peerReplica;
    Failuredetector.FailureCheck.Builder failureCheckMsg;

    UnicastDisseminationRunnable (PeerReplica peerReplica, Failuredetector.FailureCheck.Builder failureCheckMsg) {
        this.peerReplica = peerReplica;
        this.failureCheckMsg = failureCheckMsg;
    }

    @Override
    public void run() {

        Failuredetector.FailureCheck update = failureCheckMsg.build();

        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
        }

        try {
            byte[] buf = new byte[65000];

            buf = update.toByteArray();

            // send
            InetAddress address = InetAddress.getByName(peerReplica.getIp());
            DatagramPacket packet = new DatagramPacket(buf, buf.length, address, peerReplica.getFailureDetectorPort());
            socket.send(packet);

        } catch (IOException e) {

            e.printStackTrace();
        }

    }
}
