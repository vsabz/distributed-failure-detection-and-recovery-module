package siemens.db.adapt.FailureDetection.failuretrigger;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.protocol.Generic;
import siemens.db.adapt.utils.PeerReplica;

import java.util.HashMap;
import java.util.List;

public interface LocalFailureTriggerStrategy {

    void handleReceivedMessage(Failuredetector.FailureCheck failureCheck);

    void handleReceivedMessageFromGossip(HashMap<String, Long> deltaTimes);

    void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap);

    boolean stop();

    int getFailureTimeoutValue();
}
