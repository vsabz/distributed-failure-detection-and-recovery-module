package siemens.db.adapt.FailureDetection.consensus;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.protocol.Generic;
import siemens.db.adapt.utils.PeerReplica;

import java.util.HashMap;
import java.util.List;

/**
 * This abstract class models consensus strategy
 * which will be used by failure detector.
 *
 */
public interface ConsensusStrategy {


    /**
     * Attaches payload of consensus algorithm to the message.
     *
     * @param failureCheckMessage message to be sent to other replicas.
     * @return message with payload
     */
    Failuredetector.FailureCheck.Builder attachPayload(Failuredetector.FailureCheck.Builder failureCheckMessage);

    /**
     * Handles received heartbeat or ping/reply message coming from other replicas.
     * @param receivedFailureCheckMessage
     */
    void handleReceivedMessage(Failuredetector.FailureCheck receivedFailureCheckMessage);

    void setReplicas(HashMap<String, PeerReplica> peerReplicaHashMap, String replicaId);

    void setPeerReplicaAsLocallySuspected(String replicaId);

    void setFailureDetectorContext(FailureDetectorContext failureDetectionContext);

    boolean attachConsensusPayloadToPeriodicSignallingMessages();
}
