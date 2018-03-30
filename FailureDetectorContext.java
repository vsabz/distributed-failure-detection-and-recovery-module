package siemens.db.adapt.FailureDetection;

import siemens.db.adapt.FailureDetection.consensus.ConsensusStrategy;
import siemens.db.adapt.FailureDetection.dissemination.DisseminationStrategy;
import siemens.db.adapt.FailureDetection.failuretrigger.AccrualDetectorSamplingWindow;
import siemens.db.adapt.FailureDetection.failuretrigger.AccrualFailureDetector;
import siemens.db.adapt.FailureDetection.failuretrigger.FailureTriggerOnTimer;
import siemens.db.adapt.FailureDetection.failuretrigger.LocalFailureTriggerStrategy;
import siemens.db.adapt.FailureDetection.signalling.SignallingStrategy;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;

import java.util.HashMap;
import java.util.List;

public class FailureDetectorContext {


    // strategy vars
    private SignallingStrategy signallingStrategy;
    private DisseminationStrategy disseminationStrategy;
    private ConsensusStrategy consensusStrategy;
    private LocalFailureTriggerStrategy localFailureTriggerStrategy;
    private FailureTriggerOnTimer failureTriggerOnTimerAsBackup;
    private FailureDetectionAndRecoveryModule failureDetectionAndRecoveryModule;

    // stores sent messages with timestamps < ClientID, <MessageID, timestamp> >
    private HashMap<Long, Long> sentMessages;

    // stores received messages with timestamps < ClientID, <MessageID, timestamp> >
    private HashMap<String, HashMap<Long, Long>> receivedMessages;

    HashMap<String, PeerReplica> peerReplicaHashMap;


    /**
     * Constructor method initiates all strategy objects.
     *
     * @param failureDetectorContextBuilder builder object
     */
    public FailureDetectorContext(FailureDetectorContextBuilder failureDetectorContextBuilder) {
        // create signalling strategy
        signallingStrategy = failureDetectorContextBuilder.getSignallingStrategy();
        signallingStrategy.setFailureDetectorContext(this);

        // create dissemination strategy
        disseminationStrategy = failureDetectorContextBuilder.getDisseminationStrategy();
        disseminationStrategy.setFailureDetectorContext(this);
        disseminationStrategy.setDisseminationTime(failureDetectorContextBuilder.getDisseminationTime());

        // create consensus strategy
        consensusStrategy = failureDetectorContextBuilder.getConsensusStrategy();

        // create failure trigger strategy
        localFailureTriggerStrategy = failureDetectorContextBuilder.getLocalFailureTriggerStrategy();

        // initiate history vars
        sentMessages = new HashMap<>();
        receivedMessages = new HashMap<>();

    }

    /**
     *
     */
    public void start(List<PeerReplica> peerReplicas, String myReplicaId) {

        peerReplicaHashMap = new HashMap<>();
        for (PeerReplica peerReplica : peerReplicas)
            peerReplicaHashMap.put(peerReplica.getPeerReplicaId(), peerReplica);

        // configure consensus strategy
        consensusStrategy.setReplicas(peerReplicaHashMap, myReplicaId);
        consensusStrategy.setFailureDetectorContext(this);

        // start failure trigger strategy
        localFailureTriggerStrategy.start(this, peerReplicaHashMap);
        if (localFailureTriggerStrategy instanceof AccrualFailureDetector) {
            // create Timer based detector as backup till Accrual have enough samples
            failureTriggerOnTimerAsBackup = new FailureTriggerOnTimer(localFailureTriggerStrategy.getFailureTimeoutValue());
            failureTriggerOnTimerAsBackup.start(this, peerReplicaHashMap);
        }

        // start listening
        disseminationStrategy.start(this, peerReplicaHashMap, myReplicaId);


    }


    /**
     *
     * Sets failure detector and initiates history hashmap for each peer replica.
     *
     * @param failureDetectionAndRecoveryModule
     */
    public void setFailureDetectionAndRecoveryModule(FailureDetectionAndRecoveryModule failureDetectionAndRecoveryModule) {
        this.failureDetectionAndRecoveryModule = failureDetectionAndRecoveryModule;

        // create new hashmap for each replica to store received messages
        for (PeerReplica peerReplica : failureDetectionAndRecoveryModule.getPeerReplicas())
            receivedMessages.put(peerReplica.getPeerReplicaId(), new HashMap<>());
    }

    public int getFailureDetectorListeningPort() {
        return failureDetectionAndRecoveryModule.getFailureDetectorListeningPort();
    }

    public SignallingStrategy getSignallingStrategy() {
        return signallingStrategy;
    }

    public DisseminationStrategy getDisseminationStrategy() {
        return disseminationStrategy;
    }

    public ConsensusStrategy getConsensusStrategy() {
        return consensusStrategy;
    }



    /**
     * Handles received message by listener.
     * Called by dissemination protocol.
     *
     * @param receivedMessage received message
     */
    public void handleReceivedMessage(Failuredetector.FailureCheck receivedMessage) {

        // ignore my own message
        if (receivedMessage.getReplicaId().equals(failureDetectionAndRecoveryModule.getReplicaId()))
            return;

        // TODO: check if replica id is valid

        // check if received message is for me
        if (receivedMessage.getDestinationReplicaId() != null
                && !receivedMessage.getDestinationReplicaId().equals("")
                && !receivedMessage.getDestinationReplicaId().equals(failureDetectionAndRecoveryModule.getReplicaId()))
            return;

        // check if our strategies match
        if (!isMatched(receivedMessage))
            return;


        // skip history if it is a REPLY message
        if (!receivedMessage.getMessageType().equals(Failuredetector.FailureCheck.MessageType.REPLY)) {
            // TODO make interaction with history synchronized
            // return if this messages already received before
            if (receivedMessages.get(receivedMessage.getReplicaId())
                    .containsKey(receivedMessage.getMessageId()))
                return;

            // TODO make interaction with history synchronized
            // add to received messages history
            receivedMessages.get(receivedMessage.getReplicaId())
                    .put(receivedMessage.getMessageId(), System.nanoTime());
        }


        // notify signalling strategy
        signallingStrategy.handleReceivedMessage(receivedMessage);

        // notify LocalFailureTriggerStrategy
        localFailureTriggerStrategy.handleReceivedMessage(receivedMessage);
        if (localFailureTriggerStrategy instanceof AccrualFailureDetector &&
                failureTriggerOnTimerAsBackup != null)
            failureTriggerOnTimerAsBackup.handleReceivedMessage(receivedMessage);

        // trigger consensus algorithm
        consensusStrategy.handleReceivedMessage(receivedMessage);
    }


    /**
     * Handles received message by Gossip listener.
     * Called by dissemination protocol.
     *
     * @param receivedMessage received message
     */
    public void handleReceivedMessageFromGossip(Failuredetector.FailureCheck receivedMessage,
                                                HashMap<String, Long> deltaTimes) {

        // ignore my own message
        if (receivedMessage.getReplicaId().equals(failureDetectionAndRecoveryModule.getReplicaId()))
            return;

        // TODO: check if replica id is valid

        // check if received message is for me
        if (receivedMessage.getDestinationReplicaId() != null
                && !receivedMessage.getDestinationReplicaId().equals("")
                && !receivedMessage.getDestinationReplicaId().equals(failureDetectionAndRecoveryModule.getReplicaId()))
            return;

        // check if our strategies match
        if (!isMatched(receivedMessage))
            return;


        // skip history if it is a REPLY message
        if (!receivedMessage.getMessageType().equals(Failuredetector.FailureCheck.MessageType.REPLY)) {
            // TODO make interaction with history synchronized
            // return if this messages already received before
            if (receivedMessages.get(receivedMessage.getReplicaId())
                    .containsKey(receivedMessage.getMessageId()))
                return;

            // TODO make interaction with history synchronized
            // add to received messages history
            receivedMessages.get(receivedMessage.getReplicaId())
                    .put(receivedMessage.getMessageId(), System.nanoTime());
        }


        // notify signalling strategy
        signallingStrategy.handleReceivedMessage(receivedMessage);


        // notify LocalFailureTriggerStrategy
        localFailureTriggerStrategy.handleReceivedMessageFromGossip(deltaTimes);
        if (localFailureTriggerStrategy instanceof AccrualFailureDetector &&
                failureTriggerOnTimerAsBackup != null)
            failureTriggerOnTimerAsBackup.handleReceivedMessageFromGossip(deltaTimes);



        // trigger consensus algorithm
        consensusStrategy.handleReceivedMessage(receivedMessage);
    }

    // TODO
    /**
     * This method checks if the Signalling, Consensus and Dissemination strategies in received message
     * matches to this replica's strategies.
     *
     * @param receivedMessage received message by listener
     * @return true if matched, false otherwise
     */
    private boolean isMatched(Failuredetector.FailureCheck receivedMessage) {

        return true;
    }

    public void setLocalFailureConsensusReachedOnPeerReplica(String peerReplicaId) {

        if (!peerReplicaHashMap.get(peerReplicaId).isLocalSuspicionConsensusReached()) {
            peerReplicaHashMap.get(peerReplicaId).setLocalSuspicionConsensusReached(true);
            System.out.println(failureDetectionAndRecoveryModule.getReplicaId() + " : Replica "
                    + peerReplicaId + " IS DEAD at time " + System.currentTimeMillis());
        }
    }

    public void setGlobalFailureConsensusReachedOnPeerReplica(String peerReplicaId) {
        if (!peerReplicaHashMap.get(peerReplicaId).isGlobalSuspicionConsensusReached()) {
            peerReplicaHashMap.get(peerReplicaId).setGlobalSuspicionConsensusReached(true);
            System.out.println(failureDetectionAndRecoveryModule.getReplicaId() + " : Replica "
                    + peerReplicaId + " GLOBAL CONSENSUS REACHED AT " + System.currentTimeMillis());
        }

    }

    /**
     * Builds message with local matrix view payload and signalling header,
     * and triggers dissemination.
     *
     * @return
     */
    public void triggerDissemination() {
        // put replica ID
        Failuredetector.FailureCheck.Builder failureCheckMessage =
                Failuredetector.FailureCheck.newBuilder().setReplicaId(failureDetectionAndRecoveryModule.getReplicaId());

        // attach signalling parameters to the message
        signallingStrategy.addSignallingHeader(failureCheckMessage);

        // attach consensus payload to the message
        consensusStrategy.attachPayload(failureCheckMessage);

        // add to sent messages history
        sentMessages.put(failureCheckMessage.getMessageId(), System.nanoTime());

        disseminationStrategy.disseminateSpecificMessage(failureCheckMessage);

    }

    /**
     * Returns the periodic dissemination message to be sent to other replica(s).
     *
     * Called by disseminator strategy.
     *
     * @return
     */
    public Failuredetector.FailureCheck.Builder getPeriodicDisseminationMessage() {
        // put replica ID
        Failuredetector.FailureCheck.Builder failureCheckMessage =
                Failuredetector.FailureCheck.newBuilder().setReplicaId(failureDetectionAndRecoveryModule.getReplicaId());

        // attach signalling parameters to the message
        signallingStrategy.addSignallingHeader(failureCheckMessage);

        // attach consensus payload to the message
        // if consensus strategy requires to do so
        if (consensusStrategy.attachConsensusPayloadToPeriodicSignallingMessages())
            consensusStrategy.attachPayload(failureCheckMessage);


        // add to sent messages history
        sentMessages.put(failureCheckMessage.getMessageId(), System.nanoTime());

        return failureCheckMessage;
    }

    /**
     * Triggers a reply message to the previously received ping message.
     * Used by PingReply signalling strategy.
     *
     * @param failureCheckBuilder
     */
    public void triggerSignallingReply(Failuredetector.FailureCheck.Builder failureCheckBuilder) {

        // put replica ID
        failureCheckBuilder.setReplicaId(failureDetectionAndRecoveryModule.getReplicaId());

        // add to sent messages history
        sentMessages.put(failureCheckBuilder.getMessageId(), System.nanoTime());

        // attach consensus payload to the message
        // if consensus strategy requires to do so
        if (consensusStrategy.attachConsensusPayloadToPeriodicSignallingMessages())
            consensusStrategy.attachPayload(failureCheckBuilder);

        // disseminate
        disseminationStrategy.disseminateSpecificMessage(failureCheckBuilder);
    }

    /**
     * Adds missing fields of given message
     * and disseminates.
     */
    public void disseminateConsensusPayload(Failuredetector.FailureCheck.Builder failureCheckBuilder) {

        // check if signalling header is in place
        signallingStrategy.addSignallingHeader(failureCheckBuilder);

        // add replica ID
        failureCheckBuilder.setReplicaId(failureDetectionAndRecoveryModule.getReplicaId());

        // disseminate
        disseminationStrategy.disseminateSpecificMessage(failureCheckBuilder);
    }

    /**
     * Used by failure trigger module to locally set specific replica as failed.
     *
     * @param replicaId failed replica ID
     */
    public void setFailed(String replicaId) {

        if (!peerReplicaHashMap.get(replicaId).isLocallySuspected()) {
            // set locally suspected
            peerReplicaHashMap.get(replicaId).setLocallySuspected(true);

            // notify dissemination strategy
            disseminationStrategy.setPeerReplicaAsLocallySuspected(replicaId);

            // notify consensus strategy
            consensusStrategy.setPeerReplicaAsLocallySuspected(replicaId);
        }


    }

    public void stopFailureTriggerOnTimer() {
        failureTriggerOnTimerAsBackup.stop();
    }

    /**
     * Stops DisseminationStrategy and Failure Trigger.
     *
     */
    public boolean stopEverything() {
        return getDisseminationStrategy().stop() && localFailureTriggerStrategy.stop();
    }
}
