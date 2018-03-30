package siemens.db.adapt.FailureDetection.dissemination.BestEffordBroadcastDissemination;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.FailureDetection.dissemination.DisseminationStrategy;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class implements broadcast dissemination algorithm.
 *
 */
public class BestEffortBroadcastDissemination implements DisseminationStrategy {

    private ExecutorService executorService;
    private FailureDetectorMulticastListener failureDetectorMulticastListener;
    private FailureDetectorContext failureDetectorContext;
    private Timer disseminationTimer;
    private int disseminationTime;



    /**
     * Constructor initialises executor service and listener, but does not start it.
     */
    public BestEffortBroadcastDissemination() {

        // initialize listener
        failureDetectorMulticastListener = new FailureDetectorMulticastListener(this);

        // create executor
        executorService = Executors.newFixedThreadPool(1);

        disseminationTimer = new Timer();
    }

    /**
     * Sets FailureDetectorContext object.
     * Used in FailureDetectorContext constructor method.
     *
     * @param failureDetectorContext FailureDetectorContext
     */
    public void setFailureDetectorContext(FailureDetectorContext failureDetectorContext) {
        this.failureDetectorContext = failureDetectorContext;
    }

    /**
     * Sets a time interval for dissemination.
     * Used in FailureDetectorContext constructor method.
     *
     * @param disseminationTime
     */
    public void setDisseminationTime(int disseminationTime) {
        this.disseminationTime = disseminationTime;
    }

    @Override
    public void setPeerReplicaAsLocallySuspected(String replicaID) {
        // not needed for broadcast dissemination
    }

    @Override
    public void setPeerReplicaAsGlobalConsensusReached(String replicaID) {
        // not needed for broadcast dissemination
    }

    /**
     * Called by the listener to handle received message.
     * Passes the received message to FailureDetectorContext.
     *
     * @param failureCheck received message
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheck) {

        failureDetectorContext.handleReceivedMessage(failureCheck);
    }

    /**
     * Returns listener's port
     *
     * @return port
     */
    @Override
    public int getFailureDetectorListeningPort() {
        return failureDetectorContext.getFailureDetectorListeningPort();
    }

    /**
     * Starts dissemination timer.
     *
     */
    public void startDisseminationTimer() {
        disseminationTimer.schedule(new TimerTask(){
            public void run() {
                disseminate();
                startDisseminationTimer();
            }
        }, disseminationTime);
    }

    /**
     * Starts listener and dissemination timer.
     */
    @Override
    public void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap, String myReplicaID) {
        failureDetectorMulticastListener.start();
        startDisseminationTimer();
    }

    /**
     * Stops listener and dissemination.
     *
     * @return boolean
     */
    public boolean stop() {
        failureDetectorMulticastListener.stopListening();
        disseminationTimer.cancel();


        return true;
    }

    /**
     *
     * @return
     */
    @Override
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * This method implements dissemination logic.
     *
     */
    @Override
    public void disseminate() {

        // disseminate to all replicas
        executorService.execute(new MulticastDisseminationRunnable(failureDetectorContext.getPeriodicDisseminationMessage(),
                failureDetectorContext.getFailureDetectorListeningPort()));
    }

    /**
     * This method is used to disseminate a specific message, e.g. reply message to a ping.
     *
     * @param failureCheckMessage message to be disseminated.
     */
    @Override
    public void disseminateSpecificMessage(Failuredetector.FailureCheck.Builder failureCheckMessage) {
        // disseminate to all replicas
        executorService.execute(new MulticastDisseminationRunnable(failureCheckMessage,
                failureDetectorContext.getFailureDetectorListeningPort()));
    }

}
