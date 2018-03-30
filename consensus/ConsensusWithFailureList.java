package siemens.db.adapt.FailureDetection.consensus;


import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/**
 *
 * This class implements consensus algorithm which uses failure lists to do its job.
 *
 */
public class ConsensusWithFailureList implements ConsensusStrategy {


    private FailureDetectorContext failureDetectorContext;

    // variables for local consensus detection
    private HashMap<String, Long> faultList;
    private HashSet<String> replicasAchievedLocalConsensusOn;

    // variables for global consensus detection
    private HashMap<String, Long> globalConsensusList;
    private HashSet<String> replicasAchievedGlobalConsensusOn;


    private String myReplicaID;
    private long ccnt = 0;

    /**
     * Constructor initializes variables.
     *
     * It takes length of an unbroken sequence of processes that have suspected replica in their failed process list as
     * an argument. This variable is used to detect consensus in the algorithm.
     *
     * @param numberOfSuspectingReplicas
     */
    public ConsensusWithFailureList(long numberOfSuspectingReplicas) {

        // initiate fault list
        faultList = new HashMap<>();
        globalConsensusList = new HashMap<>();

        // initiate consensus list
        replicasAchievedLocalConsensusOn = new HashSet<>();
        replicasAchievedGlobalConsensusOn = new HashSet<>();

        this.ccnt = numberOfSuspectingReplicas;
    }

    /**
     * This method attaches consensus payload to the given message.
     * Consensus payload is local fault list and, if not empty, global consensus view list.
     *
     * @param failureCheckMessage message to be sent to other replicas.
     * @return given message + consensus payload
     */
    @Override
    public Failuredetector.FailureCheck.Builder attachPayload(Failuredetector.FailureCheck.Builder failureCheckMessage) {

        if (globalConsensusList.size() > 0)
            failureCheckMessage
                    .setConsensusPayloadIsGlobalView(true)
                    .addAllGlobalConsensusListPayload(globalConsensusList.keySet());
        else
            failureCheckMessage.setConsensusPayloadIsGlobalView(false);

        return failureCheckMessage
                .addAllFaultListPayload(faultList.keySet())
                .setConsensusStrategy(Failuredetector.FailureCheck.ConsensusStrategy.FAILURE_LIST);
    }

    // TODO: synchronous method due to access to fault list .. ?
    /**
     * This method calls respective methods to merge received fault and global consensus lists.
     *
     * @param receivedFailureCheckMessage
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck receivedFailureCheckMessage) {
        //System.out.println(receivedFailureCheckMessage.getConsensusPayloadIsGlobalView());

        // merge fault lists
        if (receivedFailureCheckMessage.getFaultListPayloadCount() > 0) {
            mergeFaultLists(receivedFailureCheckMessage.getFaultListPayloadList());
        }

        // merge global consensus view lists
        if (receivedFailureCheckMessage.getConsensusPayloadIsGlobalView() &&
                receivedFailureCheckMessage.getGlobalConsensusListPayloadCount() > 0) {
            mergeGlobalConsensusViewLists(receivedFailureCheckMessage.getGlobalConsensusListPayloadList());
        }

    }


    /**
     * This method merges received fault list and local fault list.
     *
     * If consensus is achieved after merge, the replica is added global consensus list.
     *
     * @param receivedFaultList received fault list
     */
    private void mergeFaultLists(List<String> receivedFaultList) {

        // will contain set of replica IDs which did not exist in received fault list
        HashSet<String> untouchedReplicas = new HashSet<>(faultList.keySet());
        untouchedReplicas.remove(myReplicaID);

        // go through received payload and merge with local fault list
        for (String peerReplicaID : receivedFaultList) {

            if (!peerReplicaID.equals(myReplicaID)) {
                // remove this id from untouced replicas list
                untouchedReplicas.remove(peerReplicaID);

                if (faultList.containsKey(peerReplicaID)) {

                    // increment ccnt if local list already contains this replica
                    // only increment in case threshold has not been passed yet, to prevent overflow
                    if (faultList.get(peerReplicaID) < ccnt) {
                        faultList.put(peerReplicaID, faultList.get(peerReplicaID) + 1);
                    }

                    // if not already achieved, check for consensus
                    if (!replicasAchievedLocalConsensusOn.contains(peerReplicaID) &&
                            faultList.get(peerReplicaID) >= ccnt) {
                        failureDetectorContext.setLocalFailureConsensusReachedOnPeerReplica(peerReplicaID);
                        replicasAchievedLocalConsensusOn.add(peerReplicaID);

                        // add to global consensus list, if not added already
                        if (!globalConsensusList.containsKey(peerReplicaID)) {
                            globalConsensusList.put(peerReplicaID, (long) 0);
                        }

                    }

                } else {
                    // add to local list with ccnt = 0
                    faultList.put(peerReplicaID, (long) 0);
                }
            }
        }

        // reset untouched replicas to 0
        for (String replicaID : untouchedReplicas) {
            faultList.put(replicaID, (long) 0);
        }
    }

    /**
     * This method merges received and local global consensus lists.
     *
     * @param receivedGlobalConsensusViewList received list
     */
    private void mergeGlobalConsensusViewLists(List<String> receivedGlobalConsensusViewList) {

        // will contain set of replica IDs which did not exist in received fault list
        HashSet<String> untouchedReplicas = new HashSet<>(globalConsensusList.keySet());
        untouchedReplicas.remove(myReplicaID);

        // go through received payload and merge with local fault list
        for (String peerReplicaID : receivedGlobalConsensusViewList) {

            if (!peerReplicaID.equals(myReplicaID)) {
                // remove this id from untouced replicas list
                untouchedReplicas.remove(peerReplicaID);

                if (globalConsensusList.containsKey(peerReplicaID)) {

                    // increment ccnt if local list already contains this replica
                    // only increment in case threshold has not been passed yet, to prevent overflow
                    if (globalConsensusList.get(peerReplicaID) < ccnt) {
                        globalConsensusList.put(peerReplicaID, globalConsensusList.get(peerReplicaID) + 1);
                    }

                    // if not already detected, check for consensus
                    if (!replicasAchievedGlobalConsensusOn.contains(peerReplicaID) &&
                            globalConsensusList.get(peerReplicaID) >= ccnt) {
                        failureDetectorContext.setGlobalFailureConsensusReachedOnPeerReplica(peerReplicaID);
                        replicasAchievedGlobalConsensusOn.add(peerReplicaID);

                        System.out.println(myReplicaID + " Global consensus reached for " + peerReplicaID + " at time  " + System.currentTimeMillis());
                    }

                    //failureDetectorContext.triggerDissemination();
                } else {
                    // add to local list with ccnt = 0
                    globalConsensusList.put(peerReplicaID, (long) 0);
                }
            }
        }

        // reset untouched replicas to 0
        for (String replicaID : untouchedReplicas) {
            globalConsensusList.put(replicaID, (long) 0);
        }
    }

    /**
     * This method is used during initialization procedure, and sets this replica's id.
     *
     * @param replicaId my own replica id
     */
    @Override
    public void setReplicas(HashMap<String, PeerReplica> peerReplicaHashMap, String replicaId) {
        // peer replicas' knowledge is not needed for this algorithm

        myReplicaID = replicaId;
    }

    /**
     * This method is used by Local Failure Trigger Strategy to set some replica as failed.
     *
     * @param replicaId failed replica.
     */
    @Override
    public void setPeerReplicaAsLocallySuspected(String replicaId) {

        if (!faultList.containsKey(replicaId)) {
            faultList.put(replicaId, (long) 0);
            //failureDetectorContext.triggerDissemination();
        }
    }

    /**
     * Sets failureDetectionContext.
     *
     * @param failureDetectionContext failureDetectionContext
     */
    @Override
    public void setFailureDetectorContext(FailureDetectorContext failureDetectionContext) {
        this.failureDetectorContext = failureDetectionContext;
    }

    /**
     * This method returns true to failureDetectionContext, specifying that consensus payload should be include in
     * periodic signalling messages.
     *
     * @return true
     */
    @Override
    public boolean attachConsensusPayloadToPeriodicSignallingMessages() {
        return true;
    }

}
