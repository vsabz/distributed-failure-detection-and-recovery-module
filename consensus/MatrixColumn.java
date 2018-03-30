package siemens.db.adapt.FailureDetection.consensus;

import siemens.db.adapt.protocol.Failuredetector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MatrixColumn {

    // this is the id of the replica above column
    private String viewedReplica;

    public String getViewedReplica() {
        return viewedReplica;
    }

    // this is the mapping between replicaIDs and their views on viewedReplica
    private Map<String, Boolean> viewersColumn;

    private boolean consensusAchieved;

    // id of the replica this matrix view belongs to
    private String myReplicaId;

    /**
     * Constructor takes the id of viewed replica.
     *
     * @param viewedReplica viewed replica id.
     */
    MatrixColumn (String viewedReplica, String myReplicaId) {
        this.viewedReplica = viewedReplica;
        this.viewersColumn = new HashMap<>();
        this.consensusAchieved = false;
        this.myReplicaId = myReplicaId;
    }

    /**
     * This method adds a new viewer to the map.
     *
     * @param viewerId id of the viewer.
     */
    public void addViewer(String viewerId) {
        viewersColumn.put(viewerId, false);
    }

    /**
     * This method builds MatrixColumn protobuf message
     * to be put in MatrixView message and transferred to remote replicas.
     *
     * @return message builder
     */
    public Failuredetector.MatrixColumn.Builder getColumnAsMessageBuilder() {

        return Failuredetector.MatrixColumn.newBuilder()
                .setViewedReplica(viewedReplica)
                .putAllMatrixColumnEntry(viewersColumn);
    }

    /**
     * This method is used to merge two matrix columns.
     *
     * It goes through the received column and replaces or ORs with existing column.
     *
     * @return true if consensus achieved, false otherwise.
     */
    public boolean mergeColumn(Failuredetector.MatrixColumn matrixColumn, String senderReplicaId,
                               AtomicBoolean changeDetected) {

        consensusAchieved = false;
        int consensusCounter = 0;

        // go through received map
        for (Map.Entry<String, Boolean> mapEntry : matrixColumn.getMatrixColumnEntryMap().entrySet()) {




            // if it is sender's own view then replace entire row
            if (mapEntry.getKey().equals(senderReplicaId)) {

                // check for change
                if (!viewersColumn.get(mapEntry.getKey()).equals(mapEntry.getValue()))
                    changeDetected.set(true);

                // replace
                viewersColumn.put(mapEntry.getKey(), mapEntry.getValue());

            }
            // OR all other rows, except my own view
            else if (!mapEntry.getKey().equals(myReplicaId)) {

                // check for change
                if (!viewersColumn.get(mapEntry.getKey())
                        .equals(mapEntry.getValue() || viewersColumn.get(mapEntry.getKey())))
                    changeDetected.set(true);

                // OR
                viewersColumn.put(mapEntry.getKey(),
                        mapEntry.getValue() || viewersColumn.get(mapEntry.getKey()));

            }


            if (viewersColumn.get(mapEntry.getKey()) && !senderReplicaId.equals(viewedReplica))
                consensusCounter++;

        }

        // check consensus
        if (consensusCounter >= viewersColumn.size() - 1) {
            consensusAchieved = true;
            //System.out.println(myReplicaId + " : " + viewedReplica + " consensusAchieved");
        }


        return consensusAchieved;
        //return changeDetected;
    }


    /**
     * This method sets viewed replica as failed for the owner replica
     * and checks for consensus.
     *
     * @return true if consensus has been achieved, false otherwise.
     *
     */
    public boolean setFailed() {
        viewersColumn.put(myReplicaId, true);
        consensusAchieved = false;

        // check for consensus
        int consensusCounter = 0;
        for (Map.Entry<String, Boolean> entry : viewersColumn.entrySet()) {
            if (entry.getValue() && !viewedReplica.equals(entry.getKey()))
                consensusCounter++;
        }
        if (consensusCounter >= viewersColumn.size() - 1)
            consensusAchieved = true;

        return consensusAchieved;
    }

    /**
     * Checks if the viewed replica is considered failed by local replica.
     *
     * @return true if considered failed, false otherwise
     *
     */
    public boolean getFailed() {
        return viewersColumn.get(myReplicaId);
    }


    /**
     * Temporary method for debug.
     */
    public void printInfo() {
        System.out.println(myReplicaId + " : Replica " + getViewedReplica() + " IS DEAD");
    }
}
