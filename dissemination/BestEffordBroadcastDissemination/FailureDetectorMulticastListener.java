package siemens.db.adapt.FailureDetection.dissemination.BestEffordBroadcastDissemination;

import siemens.db.adapt.protocol.Failuredetector;

import java.io.IOException;
import java.net.*;

public class FailureDetectorMulticastListener extends Thread {

    private boolean runFlag;
    //private int FDPort;
    //private String myReplicaId;
    private BestEffortBroadcastDissemination broadcastDisseminator;

    /**
     * Constructor gets port number to listen to.
     *
     * @param broadcastDisseminator broadcastDisseminator.
     */
    FailureDetectorMulticastListener(BestEffortBroadcastDissemination broadcastDisseminator) {
        runFlag = true;
        //FDPort = port;
        //this.myReplicaId = myReplicaId;
        //this.failureDetector = failureDetector;
        this.broadcastDisseminator = broadcastDisseminator;
    }

    public boolean isRunning() {
        return runFlag;
    }

    /**
     * Stops listening hence terminating the thread.
     *
     * @return boolean
     */
    public boolean stopListening() {
        this.runFlag = false;

        return true;
    }

    /**
     * Receives the message and passes it to broadcastDisseminator.
     */
    @Override
    public void run() {

        try {
            MulticastSocket socket = new MulticastSocket(broadcastDisseminator.getFailureDetectorListeningPort());
            InetAddress address = InetAddress.getByName("230.0.0.1");
            socket.joinGroup(address);

            DatagramPacket packet;


            while (runFlag) {
                byte[] buf = new byte[65000];
                packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                byte[] packetData = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), 0, packetData, 0, packet.getLength());
                //packetData = packet.getData();


                //Generic.FailureCheck parsedMessage = Generic.FailureCheck.parseFrom(trim(packet.getData()));
                Failuredetector.FailureCheck parsedMessage = Failuredetector.FailureCheck.parseFrom(packetData);
                broadcastDisseminator.handleReceivedMessage(parsedMessage);
            }

            socket.leaveGroup(address);
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
