package siemens.db.adapt.FailureDetection.dissemination;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * Abstract class for dissemination strategy.
 * It is extended by Broadcast and RRGossip classes.
 *
 */
public interface DisseminationStrategy {

    /**
     * Executors getter method.
     *
     * @return executors
     */
    ExecutorService getExecutorService();

    /**
     * Method for information dissemination.
     *
     */
    void disseminate();

    /**
     * Disseminates given message.
     */
    void disseminateSpecificMessage(Failuredetector.FailureCheck.Builder failureCheckMessage);

    void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap, String myReplicaId);

    boolean stop();

    void setFailureDetectorContext(FailureDetectorContext failureDetectorContext);

    void handleReceivedMessage(Failuredetector.FailureCheck failureCheck);

    int getFailureDetectorListeningPort();

    void setDisseminationTime(int time);

    void setPeerReplicaAsLocallySuspected(String replicaID);

    void setPeerReplicaAsGlobalConsensusReached(String replicaID);
}
