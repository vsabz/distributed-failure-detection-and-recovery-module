package siemens.db.adapt.FailureDetection;

import siemens.db.adapt.utils.PeerReplica;
import siemens.db.adapt.utils.Replica;

import java.util.List;


public class FailureDetectionAndRecoveryModule {

    private Replica replica;
    private FailureDetectorContext failureDetectorContext;

    /**
     * Constructor initiates variables and starts listener.
     *
     * @param failureDetectorContext context of this failure detector
     * @param replica parent replica of this failure detector
     */
    public FailureDetectionAndRecoveryModule(FailureDetectorContext failureDetectorContext, Replica replica) {
        // init variables

        this.replica = replica;
        this.failureDetectorContext = failureDetectorContext;
        this.failureDetectorContext.setFailureDetectionAndRecoveryModule(this);

    }

    /**
     * Returns list of peer replicas.
     *
     * @return list of peer replicas.
     */
    public List<PeerReplica> getPeerReplicas() {
        return replica.getPeerReplicas();
    }



    /**
     * Returns failure detector listener port number.
     * @return port number.
     */
    public int getFailureDetectorListeningPort() {
        return replica.getFailureDetectorListeningPort();
    }

    /**
     * Returns ID of parent replica.
     *
     * @return ID of parent replica..
     */
    public String getReplicaId() {
        return replica.getId();
    }

    /**
     * This method starts the listener and triggers dissemination timer.
     */
    public void start() {

        /*// configure consensus strategy
        failureDetectorContext.getConsensusStrategy().setReplicas(replica.getPeerReplicas(), replica.getId());
        //failureDetectorContext.getConsensusStrategy().setReplicaId(replica.getId());

        // configure failure trigger strategy
        failureDetectorContext.getLocalFailureTriggerStrategy().start(failureDetectorContext, replica.getPeerReplicas());

        // start listening
        failureDetectorContext.getDisseminationStrategy().startListening();

        // start dissemination
        failureDetectorContext.startDisseminationTimer();*/

        // start context
        failureDetectorContext.start(replica.getPeerReplicas(), replica.getId());
    }

    /**
     * Used to stop the listener.
     */
    public boolean stop() {
        return failureDetectorContext.stopEverything();
    }




}
