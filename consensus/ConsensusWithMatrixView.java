package siemens.db.adapt.FailureDetection.consensus;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.FailureDetection.dissemination.RoundRobinGossipDissemination.RRGossipDissemination;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class implements the consensus algorithm
 * which uses a general view matrix.
 */
public class ConsensusWithMatrixView implements ConsensusStrategy {

    // hashmap as a view matrix
    private Map<String, MatrixColumn> viewMatrix;

    // hashmap for global consensus view
    private Map<String, MatrixColumn> globalConsensusViewMatrix;

    FailureDetectorContext failureDetectorContext;
    private String myReplicaID;

    /**
     * Attaches payload of consensus algorithm to the message.
     *
     * @param failureCheckMessage message to be sent to other replicas.
     * @return message with payload
     */
    @Override
    public Failuredetector.FailureCheck.Builder attachPayload(Failuredetector.FailureCheck.Builder failureCheckMessage) {

        Failuredetector.MatrixView.Builder matrixViewBuilder = Failuredetector.MatrixView.newBuilder();

        // populate MatrixView message
        for (MatrixColumn matrixColumn : viewMatrix.values())
            matrixViewBuilder.addMatrixColumn(matrixColumn.getColumnAsMessageBuilder().build());

        return failureCheckMessage
                .setMatrixViewPayload(matrixViewBuilder.build())
                .setConsensusStrategy(Failuredetector.FailureCheck.ConsensusStrategy.MATRIX_VIEW)
                .setConsensusPayloadIsGlobalView(false);
    }

    /**
     * Triggers dissemination of global consensus view matrix.
     *
     * @return message with payload
     */
    public void triggerGlobalConsensusViewDissemination() {

        Failuredetector.MatrixView.Builder matrixViewBuilder2 = Failuredetector.MatrixView.newBuilder();

        // populate MatrixView message
        for (MatrixColumn matrixColumn : globalConsensusViewMatrix.values())
            matrixViewBuilder2.addMatrixColumn(matrixColumn.getColumnAsMessageBuilder().build());

        failureDetectorContext.disseminateConsensusPayload(Failuredetector.FailureCheck.newBuilder()
                .setMatrixViewPayload(matrixViewBuilder2)
                .setConsensusStrategy(Failuredetector.FailureCheck.ConsensusStrategy.MATRIX_VIEW)
                .setConsensusPayloadIsGlobalView(true));

    }

    /**
     * Handles received heartbeat or ping/reply message coming from other replicas.
     * @param receivedFailureCheckMessage
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck receivedFailureCheckMessage) {

        if (receivedFailureCheckMessage.getConsensusPayloadIsGlobalView())
            handleGlobalConsensusView(receivedFailureCheckMessage);
        else
            handleLocalConsensusView(receivedFailureCheckMessage);

    }

    /**
     * Merges received matrix with local one.
     *
     * Called if the matrix contains local failure view of the sender.
     *
     * @param receivedFailureCheckMessage
     */
    public void handleLocalConsensusView(Failuredetector.FailureCheck receivedFailureCheckMessage) {

        // TODO optimize: thread pool?
        // merge received view matrix
        AtomicBoolean changeDetectedLocalMatrix = new AtomicBoolean(false);
        boolean changeDetectedInGlobalMatrix = false;

        for (Failuredetector.MatrixColumn matrixColumn
                : receivedFailureCheckMessage.getMatrixViewPayload().getMatrixColumnList()) {

            // merge received MatrixColumn with local MatrixColumn
            Boolean localConsensusAchieved = viewMatrix.get(matrixColumn.getViewedReplica()).mergeColumn(matrixColumn,
                    receivedFailureCheckMessage.getReplicaId(), changeDetectedLocalMatrix);

            // check if merged MatrixColumn reached local consensus
            if (localConsensusAchieved) {
                // notify PeerReplica object
                failureDetectorContext.setLocalFailureConsensusReachedOnPeerReplica(matrixColumn.getViewedReplica());

                // set flag in global consensus matrix and disseminate, if not already set
                if (!globalConsensusViewMatrix.get(matrixColumn.getViewedReplica()).getFailed()) {
                    boolean globalConsensusAchieved = globalConsensusViewMatrix.get(matrixColumn.getViewedReplica()).setFailed();
                    if (globalConsensusAchieved)
                        failureDetectorContext.setGlobalFailureConsensusReachedOnPeerReplica(matrixColumn.getViewedReplica());
                    changeDetectedInGlobalMatrix = true;

                }
            }
        }

        // trigger dissemination if there was a change in local matrix view
        if (changeDetectedLocalMatrix.get()) {
            failureDetectorContext.triggerDissemination();
            //System.out.println("CHANGE DETECTED IN MATRIX VIEW");
        }

        if (changeDetectedInGlobalMatrix)
            triggerGlobalConsensusViewDissemination();

    }

    /**
     * Merges received matrix with local one.
     *
     * Called if the matrix contains global consensus view.
     *
     * @param receivedFailureCheckMessage
     */
    public void handleGlobalConsensusView(Failuredetector.FailureCheck receivedFailureCheckMessage) {
        // TODO optimize: thread pool?
        // merge received global consensus view matrix
        AtomicBoolean changeDetected = new AtomicBoolean(false);
        for (Failuredetector.MatrixColumn matrixColumn
                : receivedFailureCheckMessage.getMatrixViewPayload().getMatrixColumnList()) {

            // merge received MatrixColumn with local MatrixColumn
            Boolean globalConsensusAchieved = globalConsensusViewMatrix.get(matrixColumn.getViewedReplica())
                    .mergeColumn(matrixColumn, receivedFailureCheckMessage.getReplicaId(), changeDetected);

            // check if merged MatrixColumn reached consensus
            if (globalConsensusAchieved) {
                // notify PeerReplica object
                failureDetectorContext.setGlobalFailureConsensusReachedOnPeerReplica(matrixColumn.getViewedReplica());

            }
        }

        // trigger dissemination if there was a change
        if (changeDetected.get()) {
            triggerGlobalConsensusViewDissemination();
        }
    }

    /**
     * Sets FailureDetectorContext object.
     *
     * @param failureDetectionContext
     */
    public void setFailureDetectorContext(FailureDetectorContext failureDetectionContext) {
        this.failureDetectorContext = failureDetectionContext;
    }

    @Override
    public boolean attachConsensusPayloadToPeriodicSignallingMessages() {
        return false;
    }

    /**
     * Creates view matrix.
     *
     * @param peerReplicaHashMap list of peer replicas
     * @param replicaId parent replica ID
     */
    @Override
    public void setReplicas(HashMap<String, PeerReplica> peerReplicaHashMap, String replicaId) {
        viewMatrix = new HashMap<>(peerReplicaHashMap.size());
        globalConsensusViewMatrix = new HashMap<>(peerReplicaHashMap.size());
        this.myReplicaID = replicaId;

        // build view matrix and fault vector
        for (PeerReplica peerReplicaViewed : peerReplicaHashMap.values()) {
            // create matrix column
            MatrixColumn matrixColumn = new MatrixColumn(peerReplicaViewed.getPeerReplicaId(), replicaId);
            MatrixColumn consensusViewColumn = new MatrixColumn(peerReplicaViewed.getPeerReplicaId(), replicaId);

            // populate matrix column
            matrixColumn.addViewer(replicaId);
            consensusViewColumn.addViewer(replicaId);
            for (PeerReplica peerReplicaViewer : peerReplicaHashMap.values()) {
                matrixColumn.addViewer(peerReplicaViewer.getPeerReplicaId());
                consensusViewColumn.addViewer(peerReplicaViewer.getPeerReplicaId());
            }

            // populate matrix
            viewMatrix.put(peerReplicaViewed.getPeerReplicaId(), matrixColumn);
            globalConsensusViewMatrix.put(peerReplicaViewed.getPeerReplicaId(), consensusViewColumn);


        }

        // create matrix entry for this replica itself
        MatrixColumn matrixColumn = new MatrixColumn(replicaId, replicaId);
        MatrixColumn consensusViewColumn = new MatrixColumn(replicaId, replicaId);

        matrixColumn.addViewer(replicaId);
        consensusViewColumn.addViewer(replicaId);

        for (PeerReplica peerReplicaViewer : peerReplicaHashMap.values()) {
            matrixColumn.addViewer(peerReplicaViewer.getPeerReplicaId());
            consensusViewColumn.addViewer(peerReplicaViewer.getPeerReplicaId());
        }

        viewMatrix.put(replicaId, matrixColumn);
        globalConsensusViewMatrix.put(replicaId, consensusViewColumn);
    }


    /**
     * Sets specific replica as failed and checks for local consensus.
     *
     * Triggers dissemination of consensus payload to other replicas afterwards.
     *
     * @param replicaId failed replica's ID
     */
    public void setPeerReplicaAsLocallySuspected(String replicaId) {
        //System.out.println(myReplicaID + " : REPLICA " + replicaId + " is locally SET FAILED ");
        boolean localConsensusAchieved = viewMatrix.get(replicaId).setFailed();
        if (localConsensusAchieved) {
            // local consensus reached
            failureDetectorContext.setLocalFailureConsensusReachedOnPeerReplica(replicaId);

            // set flag in global consensus matrix and disseminate, if not already set
            if (!globalConsensusViewMatrix.get(replicaId).getFailed()) {
                boolean globalConsensusAchieved = globalConsensusViewMatrix.get(replicaId).setFailed();
                if (globalConsensusAchieved)
                    failureDetectorContext.setGlobalFailureConsensusReachedOnPeerReplica(replicaId);
                triggerGlobalConsensusViewDissemination();
            }
        }

        failureDetectorContext.triggerDissemination();

        // remove this replica from gossip list
        // so that gossip message wouldn't be sent to unreachable destination
        /*if (failureDetectorContext.getDisseminationStrategy() instanceof RRGossipDissemination) {
            ((RRGossipDissemination) failureDetectorContext.getDisseminationStrategy()).setPeerReplicaAsLocallySuspected(replicaId);
        }*/

    }


}
