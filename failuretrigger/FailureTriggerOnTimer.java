package siemens.db.adapt.FailureDetection.failuretrigger;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;

import java.util.*;

/**
 *
 */
public class FailureTriggerOnTimer implements LocalFailureTriggerStrategy {

    private Map<String, Timer> timers;
    private Map<String, TimerTask> timerTasks;
    private FailureDetectorContext failureDetectorContext;
    private int failureTimeout;
    private boolean stopped;


    /**
     *
     * @param failureTimeout
     */
    public FailureTriggerOnTimer(int failureTimeout) {
        this.timers = new HashMap<>();
        this.timerTasks = new HashMap<>();
        this.failureTimeout = failureTimeout;
        this.stopped = false;
    }

    /**
     * Starts timers for each replica.
     *
     * @param peerReplicaHashMap
     */
    public void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap) {

        this.failureDetectorContext = failureDetectorContext;

        for (Map.Entry<String, PeerReplica> entry : peerReplicaHashMap.entrySet()) {

            // create Timer and TimerTask
            Timer timer = new Timer();
            TimerTask timerTask = new TimerTask(){
                public void run() {

                    // trigger failure
                    failureDetectorContext.setFailed(entry.getKey());
                }
            };
            timers.put(entry.getKey(), timer);
            timerTasks.put(entry.getKey(), timerTask);

            // schedule
            timer.schedule(timerTask, failureTimeout);
        }
    }

    /**
     *
     * Resets timer of specific replica when a message has been received from it.
     *
     * @param failureCheck received message
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheck) {
        //timers.get(failureCheck.getReplicaId()).cancel();
        if (stopped)
            return;

        // cancel current TimerTask and create new one
        timerTasks.get(failureCheck.getReplicaId()).cancel();
        TimerTask timerTask = new TimerTask(){
            public void run() {
                failureDetectorContext.setFailed(failureCheck.getReplicaId());
            }
        };
        timerTasks.put(failureCheck.getReplicaId(), timerTask);

        //Timer timer = new Timer();
        //timers.put(failureCheck.getReplicaId(), timer);

        // get Timer object
        Timer timer = timers.get(failureCheck.getReplicaId());

        // create new Timer if terminated
        if (timer == null) {
            timer = new Timer();
            timers.put(failureCheck.getReplicaId(), timer);
        }

        // schedule
        timer.purge();
        timer.schedule(timerTask, failureTimeout);
    }

    /**
     *
     * @param deltaTimes
     */
    @Override
    public void handleReceivedMessageFromGossip(HashMap<String, Long> deltaTimes) {
        //timers.get(failureCheck.getReplicaId()).cancel();
        if (stopped)
            return;

        // TODO: why are we creating new TimerTasks? Use existing ones?
        for (Map.Entry<String, Long> entry : deltaTimes.entrySet()) {
            if (failureTimeout - entry.getValue() > 0) {
                // cancel current TimerTask and create new one
                timerTasks.get(entry.getKey()).cancel();
                TimerTask timerTask = new TimerTask() {
                    public void run() {
                        failureDetectorContext.setFailed(entry.getKey());
                    }
                };
                timerTasks.put(entry.getKey(), timerTask);

                // get Timer object
                Timer timer = timers.get(entry.getKey());

                // create new Timer if terminated
                if (timer == null) {
                    timer = new Timer();
                    timers.put(entry.getKey(), timer);
                }

                // schedule
                timer.purge();
                timer.schedule(timerTask, failureTimeout - entry.getValue());
            }
        }
    }

    /**
     * Stops timers.
     */
    @Override
    public boolean stop() {

        stopped = true;

        for (Timer timer : timers.values())
            timer.cancel();

        return true;
    }

    @Override
    public int getFailureTimeoutValue() {
        return failureTimeout;
    }
}
