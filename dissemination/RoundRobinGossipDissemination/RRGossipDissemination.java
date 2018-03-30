package siemens.db.adapt.FailureDetection.dissemination.RoundRobinGossipDissemination;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.FailureDetection.dissemination.DisseminationStrategy;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;
import siemens.db.adapt.utils.PropertiesReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class models Round Robin Gossip protocol
 * which is used for information dissemination.
 */
public class RRGossipDissemination implements DisseminationStrategy {


    private ExecutorService executorService;
    private FailureDetectorContext failureDetectorContext;
    // hashmap contaning peer replicas <peerReplicaID, PeerReplica>
    private HashMap<String, PeerReplica> peerReplicaHashMap;
    // array containing peer replica objects sorted by their IDs
    private String[] sortedPeerReplicaIDs;
    // my own replica id
    private String myReplicaID;
    // dissemination triggered every gossip time
    private int gossipTime;
    // used to determine destination replica id
    private int gossipRound;
    private Timer gossipTimer;
    private int myReplicaIndexInSortedArray;
    private int log2n;
    private FailureDetectorUnicastListener failureDetectorUnicastListener;
    //private HashMap<String, Long> deltaTimestampOfReplicas;
    private HashMap<String, Long> latesTimestampOfReplicas;
    private Object sortedArrayLock;

    // list for locally suspected replicas
    private HashSet<String> suspectedReplicas;

    /**
     * Constructor creates pool of executor threads, gossip timer,
     * and listener object.
     *
     */
    public RRGossipDissemination() {

        gossipTimer = new Timer();
        failureDetectorUnicastListener = new FailureDetectorUnicastListener(this);
        suspectedReplicas = new HashSet<>();
        sortedArrayLock = new Object();

        try {
            executorService = Executors.newFixedThreadPool(Integer.valueOf(new PropertiesReader().getProperty("default_thread_count")));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * Returns executors.
     *
     * @return
     */
    @Override
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Dissemination method.
     *
     * Destination replica is chosen based on round robin gossip protocol.
     *
     * destinationID = sourceID + 2pow(GossipRound - 1).
     *
     * This method asks for a message from failuredetectioncontext object,
     * and adds its own payload to it. Payload consists of delta timestamps of received heartbeats from
     * peer replicas.
     *
     */
    @Override
    public synchronized void disseminate() {


        // calculate delta times and attach to payload
        HashMap<String, Long> deltaTimestampOfReplicas = new HashMap<>();
        long currentTimeMillis = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : latesTimestampOfReplicas.entrySet()) {
            deltaTimestampOfReplicas.put(entry.getKey(), currentTimeMillis - entry.getValue());
        }

        // send to next replica in BRR fasion
        int destionationReplicaIndex = myReplicaIndexInSortedArray + new Double(Math.pow(2, gossipRound-1)).intValue();
        synchronized (sortedArrayLock) {
            while (destionationReplicaIndex >= sortedPeerReplicaIDs.length)
                destionationReplicaIndex -= sortedPeerReplicaIDs.length;
            if (!sortedPeerReplicaIDs[destionationReplicaIndex].equals(myReplicaID)) {
                executorService.execute(new UnicastDisseminationRunnable(
                        peerReplicaHashMap.get(sortedPeerReplicaIDs[destionationReplicaIndex]),
                        failureDetectorContext.getPeriodicDisseminationMessage().putAllGossipPayloadDeltaTimestampsOfReplicas(deltaTimestampOfReplicas)));
            }
        }

        // send to suspected replicas too
        for (String replicaID : suspectedReplicas) {
            executorService.execute(new UnicastDisseminationRunnable(
                    peerReplicaHashMap.get(replicaID),
                    failureDetectorContext.getPeriodicDisseminationMessage().putAllGossipPayloadDeltaTimestampsOfReplicas(deltaTimestampOfReplicas)));
        }

        gossipRound ++;
        if  (gossipRound > log2n)
            gossipRound = 1;
    }

    /**
     * Disseminates specific message.
     *
     * If the message does not includes destination replica,
     * destination replica is chosen based on round robin gossip protocol.
     * However, this is not being counted as a gossip round.
     *
     * destinationID = sourceID + 2pow(GossipRound - 1).
     *
     * This method asks for a message from failuredetectioncontext object,
     * and adds its own payload to it. Payload consists of delta timestamps of received heartbeats from
     * peer replicas.
     *
     */
    @Override
    public void disseminateSpecificMessage(Failuredetector.FailureCheck.Builder failureCheckMessage) {

        // calculate delta times and attach to payload
        HashMap<String, Long> deltaTimestampOfReplicas = new HashMap<>();
        long currentTimeMillis = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : latesTimestampOfReplicas.entrySet()) {
            deltaTimestampOfReplicas.put(entry.getKey(), currentTimeMillis - entry.getValue());
        }

        // message does not contain destination replica, so send arbitrary replica in BRR fasion
        if (failureCheckMessage.getDestinationReplicaId().isEmpty() &&
                failureCheckMessage.getDestinationReplicaId().equals("")) {
            int destionationReplicaIndex = myReplicaIndexInSortedArray + new Double(Math.pow(2, gossipRound - 1)).intValue();

            synchronized (sortedArrayLock) {
                while (destionationReplicaIndex >= sortedPeerReplicaIDs.length)
                    destionationReplicaIndex -= sortedPeerReplicaIDs.length;

                //System.out.println("Sending specific message from " + myReplicaID + " to " + sortedPeerReplicaIDs[destionationReplicaIndex]);
                if (!sortedPeerReplicaIDs[destionationReplicaIndex].equals(myReplicaID)) {
                    executorService.execute(new UnicastDisseminationRunnable(
                            peerReplicaHashMap.get(sortedPeerReplicaIDs[destionationReplicaIndex]),
                            failureCheckMessage.putAllGossipPayloadDeltaTimestampsOfReplicas(deltaTimestampOfReplicas)));
                }
            }

            // send to suspected replicas too
            for (String replicaID : suspectedReplicas) {
                //System.out.println("Sending specific message from " + myReplicaID + " to " + replicaID);
                executorService.execute(new UnicastDisseminationRunnable(
                        peerReplicaHashMap.get(replicaID),
                        failureCheckMessage.putAllGossipPayloadDeltaTimestampsOfReplicas(deltaTimestampOfReplicas)));
            }

        }
        // only send to destination replica specified in the message
        else {

            executorService.execute(new UnicastDisseminationRunnable(
                    peerReplicaHashMap.get(peerReplicaHashMap.get(failureCheckMessage.getDestinationReplicaId())),
                    failureCheckMessage.putAllGossipPayloadDeltaTimestampsOfReplicas(deltaTimestampOfReplicas)));
        }




        // iterate gossip round
        gossipRound ++;
        if  (gossipRound > log2n)
            gossipRound = 1;

    }

    /**
     * Initiates vars and sorts peer replicas in an array
     * for easier usage in gossip protocol.
     *
     * @param failureDetectorContext
     * @param peerReplicaHashMap
     */
    @Override
    public void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap, String myReplicaID) {
        this.failureDetectorContext = failureDetectorContext;
        this.peerReplicaHashMap = peerReplicaHashMap;
        this.myReplicaID = myReplicaID;
        this.gossipRound = 1;
        //this.deltaTimestampOfReplicas = new HashMap<>();
        this.latesTimestampOfReplicas = new HashMap<>();

        sortedPeerReplicaIDs = new String[peerReplicaHashMap.size() + 1];
        sortedPeerReplicaIDs[0] = myReplicaID;
        int i = 1;
        for (String peerReplicaID : peerReplicaHashMap.keySet()) {
            sortedPeerReplicaIDs[i] = peerReplicaID;
            i++;
        }
        Arrays.sort(sortedPeerReplicaIDs);

        for (int k = 0; k < sortedPeerReplicaIDs.length; k++) {
            //System.out.println(myReplicaID + " : [" + k + "] : " + sortedPeerReplicaIDs[k]);
            if (sortedPeerReplicaIDs[k].equals(myReplicaID))
                myReplicaIndexInSortedArray = k;
        }

        log2n = (int)Math.floor(Math.log(sortedPeerReplicaIDs.length) / Math.log(2));

        startDisseminationTimer();

        failureDetectorUnicastListener.start();

    }

    /**
     * Starts dissemination timer.
     *
     */
    public void startDisseminationTimer() {
        gossipTimer.schedule(new TimerTask(){
            public void run() {
                disseminate();
                startDisseminationTimer();
            }
        }, gossipTime);
    }


    /**
     * Stops listener and timer.
     *
     * @return
     */
    @Override
    public boolean stop() {
        failureDetectorUnicastListener.stopListening();
        gossipTimer.cancel();

        return true;
    }

    @Override
    public void setFailureDetectorContext(FailureDetectorContext failureDetectorContext) {

    }

    /**
     * Handles received message from peer gossip protocol.
     *
     * Payload contains delta times of heartbeats of other replicas that sender of this update received.
     *
     * This method uses those delta times to figure out actual timestamp and if it is newer or not.
     *
     * Then calls handler method of failure detector context.
     *
     * @param failureCheck
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheck) {
        // go through timestamps and update local DELTA and TIMESTAMP lists
        HashMap<String, Long> deltaTimestampOfReplicas = new HashMap<>();
        deltaTimestampOfReplicas.put(failureCheck.getReplicaId(), (long)0);
        long currentTimeMillis = System.currentTimeMillis();
        latesTimestampOfReplicas.put(failureCheck.getReplicaId(), currentTimeMillis);
        //System.out.println(myReplicaID + " : delta values from " + failureCheck.getReplicaId() + " : " + failureCheck.getGossipPayloadDeltaTimestampsOfReplicasMap());

        if (failureCheck.getGossipPayloadDeltaTimestampsOfReplicasMap() != null) {
            for (Map.Entry<String, Long> entry : failureCheck.getGossipPayloadDeltaTimestampsOfReplicasMap().entrySet()) {

                if (latesTimestampOfReplicas.containsKey(entry.getKey())) {
                    // if received timestamp is AFTER my own timestamp
                    if (currentTimeMillis - entry.getValue() > latesTimestampOfReplicas.get(entry.getKey())) {
                        latesTimestampOfReplicas.put(entry.getKey(), currentTimeMillis - entry.getValue());
                        deltaTimestampOfReplicas.put(entry.getKey(), entry.getValue());
                    }
                } else if (!entry.getKey().equals(myReplicaID)) {
                    // I have no information about this replica
                    latesTimestampOfReplicas.put(entry.getKey(), currentTimeMillis - entry.getValue());
                    deltaTimestampOfReplicas.put(entry.getKey(), entry.getValue());
                }
            }
        }

        //System.out.println(myReplicaID + " : delta values from " + failureCheck.getReplicaId() + " : " + deltaTimestampOfReplicas);

        //if (myReplicaID.equals("[B@470e2030127.0.0.19005")) {

        failureDetectorContext.handleReceivedMessageFromGossip(failureCheck, deltaTimestampOfReplicas);
    }

    /**
     * Returns listening port.
     *
     * @return
     */
    @Override
    public int getFailureDetectorListeningPort() {
        return failureDetectorContext.getFailureDetectorListeningPort();
    }

    /**
     * Sets gossip time.
     *
     * @param time
     */
    @Override
    public void setDisseminationTime(int time) {
        this.gossipTime = time;
    }

    /**
     * This method removes a replica from gossiping list
     * and calculates log2n => max round number.
     *
     * Removed replica is added to suspected replica map.
     *
     * @param replicaID
     */
    @Override
    public void setPeerReplicaAsLocallySuspected(String replicaID) {
        // remove the replica from sorted array and copy the array to smaller array
        //System.out.println(myReplicaID + ": " +  replicaID + " is added to suspect list");


        // if not already in the failed replicas list
        if (!suspectedReplicas.contains(replicaID)) {
            synchronized (sortedArrayLock) {
                String[] tempArray = new String[sortedPeerReplicaIDs.length - 1];
                int arrayIndex = 0;
                for (int i = 0; i < sortedPeerReplicaIDs.length; i++) {
                    //System.out.println();
                    if (!sortedPeerReplicaIDs[i].equals(replicaID)) {
                        tempArray[arrayIndex] = sortedPeerReplicaIDs[i];
                        arrayIndex++;
                    } else {
                        // add to suspected replicas list
                        suspectedReplicas.add(replicaID);
                    }
                }
                sortedPeerReplicaIDs = tempArray;

                // recalculate log2n
                log2n = (int) Math.floor(Math.log(sortedPeerReplicaIDs.length) / Math.log(2));
            }

            //System.out.println(myReplicaID + " suspect list : " + suspectedReplicas);
        }
    }

    /**
     * Removes the replica's id from suspection list when global consensus is reached,
     * so no more gossip messages will be sent to that replica.
     *
     * @param replicaID
     */
    @Override
    public void setPeerReplicaAsGlobalConsensusReached(String replicaID) {
        suspectedReplicas.remove(replicaID);
    }


}
