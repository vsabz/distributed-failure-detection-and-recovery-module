package siemens.db.adapt.FailureDetection.failuretrigger;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.utils.PeerReplica;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class AccrualFailureDetector implements LocalFailureTriggerStrategy {

    private Map<String, Timer> timers;
    private Map<String, TimerTask> timerTasks;
    private FailureDetectorContext failureDetectorContext;
    private int recalculationTimeInMillis;
    private int suspicionThreshold;
    private int samplingWindowSize;
    private HashMap<String, AccrualDetectorSamplingWindow> peerReplicaDeltaTimeSamples;
    private HashMap<String, Long> peerReplicaLastReceptionTimestamp;
    private boolean failureTriggerOnTimerStopped;
    private Timer samplingWindowCheckerTimer;

    private boolean stopped;
    // for debug
    //String replica3 = "";

    /**
     *
     * @param suspicionThreshold
     * @param samplingWindowSize
     */
    public AccrualFailureDetector(int suspicionThreshold, int samplingWindowSize, int recalculationTimeInMillis) {
        this.suspicionThreshold = suspicionThreshold;
        this.samplingWindowSize = samplingWindowSize;
        this.peerReplicaDeltaTimeSamples = new HashMap<>();
        this.peerReplicaLastReceptionTimestamp = new HashMap<>();
        this.recalculationTimeInMillis = recalculationTimeInMillis;
        this.timers = new HashMap<>();
        this.timerTasks = new HashMap<>();
        this.failureTriggerOnTimerStopped = false;



        //System.out.println(-1.0 * Math.log10(1.0 - d.cumulativeProbability(5)));

    }

    /**
     * Handles the received message by recording arrival time
     * and creaing a new sample.
     *
     * @param failureCheck received message
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheck) {
        if (stopped)
            return;

        long currentTimeMillis = System.currentTimeMillis();


        if (peerReplicaLastReceptionTimestamp.containsKey(failureCheck.getReplicaId())) {

            // add to samples
            peerReplicaDeltaTimeSamples.get(failureCheck.getReplicaId())
                    .addNewSample(currentTimeMillis - peerReplicaLastReceptionTimestamp.get(failureCheck.getReplicaId()));
            //if (failureCheck.getReplicaId().equals(replica3)) {
                //System.out.println("Adding sample " + (currentTimeMillis - peerReplicaLastReceptionTimestamp.get(replica3) + " for replica 3"));
                //System.out.println("Suspicion value : " + peerReplicaDeltaTimeSamples.get(replica3).phiValue(peerReplicaLastReceptionTimestamp.get(replica3)) +
                //        " threshold : " + suspicionThreshold);
            //}
        }

        // record timestamp
        peerReplicaLastReceptionTimestamp.put(failureCheck.getReplicaId(), currentTimeMillis);

        // debug
        //System.out.println("phi value for replica 3 :" + peerReplicaDeltaTimeSamples.get("[B@51931956127.0.0.19003").phiValue());
        //System.out.println("phi value for replica 3 :" + peerReplicaDeltaTimeSamples.get(replica3).phiValue(peerReplicaLastReceptionTimestamp.get(replica3)));

    }

    /**
     * This method handles received delta timestamps and updates samples.
     * Used when dissemination strategy is Gossip.
     *
     * @param deltaTimes
     */
    @Override
    public void handleReceivedMessageFromGossip(HashMap<String, Long> deltaTimes) {
        if (stopped)
            return;

        long currentTimeMillis = System.currentTimeMillis();

        for (Map.Entry<String, Long> entry : deltaTimes.entrySet()) {
            // add new sample


            if (peerReplicaLastReceptionTimestamp.containsKey(entry.getKey())) {
                peerReplicaDeltaTimeSamples.get(entry.getKey()).addNewSample(
                        (currentTimeMillis - entry.getValue()) - peerReplicaLastReceptionTimestamp.get(entry.getKey()));


                System.out.println("Adding delta for " + entry.getKey() + ": " + ((currentTimeMillis - entry.getValue()) - peerReplicaLastReceptionTimestamp.get(entry.getKey())));
            }
            // update last received message's timestamp
            peerReplicaLastReceptionTimestamp.put(entry.getKey(), currentTimeMillis - entry.getValue());
        }

    }

    /**
     * Creates sampling windows for each peer replica.
     *
     * There are 2 timers here:
     *
     * One of them is used to calculate phi values of each peer replica in regular time intervals.
     *
     * Other one is used to check whether all sampling windows have enough samples to start operating.
     * If yes, stop default triggerer strategy.
     *
     * @param failureDetectorContext
     * @param peerReplicaHashMap
     */
    @Override
    public void start(FailureDetectorContext failureDetectorContext, HashMap<String, PeerReplica> peerReplicaHashMap) {
        this.failureDetectorContext = failureDetectorContext;
        stopped = false;

        // create sampling window for each replica
        for (String peerReplicaID : peerReplicaHashMap.keySet()) {

            AccrualDetectorSamplingWindow samplingWindow = new AccrualDetectorSamplingWindow(samplingWindowSize);
            peerReplicaDeltaTimeSamples.put(peerReplicaID, samplingWindow);
            //System.out.println(peerReplicaID);

            // for debug
            /*if (peerReplicaHashMap.get(peerReplicaID).getFailureDetectorPort() == 7003)
                replica3 = peerReplicaID;*/


            // create Timer and TimerTask for checking phi values
            Timer timer = new Timer();
            TimerTask timerTask = new TimerTask(){
                public void run() {



                    if (failureTriggerOnTimerStopped) {


                        double phiValue = samplingWindow.phiValue(peerReplicaLastReceptionTimestamp.get(peerReplicaID));
                        //if (peerReplicaID.equals(replica3))
                            //System.out.println("Suspicion value : " + phiValue +
                            //    " threshold : " + suspicionThreshold);
                        if ((phiValue == Double.POSITIVE_INFINITY) || (phiValue > suspicionThreshold)) {
                            // trigger failure
                            failureDetectorContext.setFailed(peerReplicaID);
                        }
                    }

                    // reschedule the timer
                    rescheduleTimer(peerReplicaID);
                }
            };
            timers.put(peerReplicaID, timer);
            timerTasks.put(peerReplicaID, timerTask);

            // schedule
            timer.schedule(timerTask, recalculationTimeInMillis);
        }


        // stop FailureTriggerOnTimer strategy if sampling windows are full
        // and Accrual can start operating
        samplingWindowCheckerTimer = new Timer();
        TimerTask timerTask = new TimerTask(){
            public void run() {

                boolean ready = true;

                for (AccrualDetectorSamplingWindow samplingWindow : peerReplicaDeltaTimeSamples.values())
                    if (!samplingWindow.isSamplingWindowFull())
                        ready = false;

                if (ready) {
                    // stop FailureTriggerOnTimerStrategy
                    failureDetectorContext.stopFailureTriggerOnTimer();
                    failureTriggerOnTimerStopped = true;
                } else {


                    //reschedule this timer
                    rescheduleSamplingWindowCheckerTimer();
                }
            }
        };
        // schedule
        samplingWindowCheckerTimer.schedule(timerTask, recalculationTimeInMillis);

    }

    /**
     * Reschedules the timer which is used to detect whether sampling windows have enough samples to start operating.
     *
     */
    void rescheduleSamplingWindowCheckerTimer() {

        TimerTask timerTask = new TimerTask(){
            public void run() {
                boolean ready = true;

                for (AccrualDetectorSamplingWindow samplingWindow : peerReplicaDeltaTimeSamples.values())
                    if (!samplingWindow.isSamplingWindowFull())
                        ready = false;

                if (ready) {
                    // stop FailureTriggerOnTimerStrategy
                    System.out.println("STOPPING TIMER BASED FAILURE DETECTOR!");
                    failureDetectorContext.stopFailureTriggerOnTimer();
                    failureTriggerOnTimerStopped = true;
                } else {
                    //reschedule this timer
                    rescheduleSamplingWindowCheckerTimer();

                }
            }
        };

        // reschedule
        samplingWindowCheckerTimer.purge();
        samplingWindowCheckerTimer.schedule(timerTask, recalculationTimeInMillis);
    }


    /**
     * Reschdules the timer which is used to calculate phi values
     * of each replica in regular time intervals.
     *
     * @param peerReplicaID
     */
    void rescheduleTimer(String peerReplicaID) {

        // cancel current TimerTask and create new one
        timerTasks.get(peerReplicaID).cancel();
        TimerTask timerTask = new TimerTask(){
            public void run() {

                if (failureTriggerOnTimerStopped) {

                    double phiValue = peerReplicaDeltaTimeSamples.get(peerReplicaID).phiValue(peerReplicaLastReceptionTimestamp.get(peerReplicaID));

                    /*if (peerReplicaID.equals(replica3)) {
                        System.out.println("FROM TIMER: Suspicion value : " + peerReplicaDeltaTimeSamples.get(replica3).phiValue(peerReplicaLastReceptionTimestamp.get(replica3)) +
                                " threshold : " + suspicionThreshold + "; last timestamp: " + peerReplicaLastReceptionTimestamp.get(replica3));
                    }*/

                    if ((phiValue == Double.POSITIVE_INFINITY) || (phiValue > suspicionThreshold)) {
                        // trigger failure
                        failureDetectorContext.setFailed(peerReplicaID);
                    }
                }

                // reschedule the timer
                rescheduleTimer(peerReplicaID);
            }
        };
        timerTasks.put(peerReplicaID, timerTask);

        // reschedule
        Timer timer = timers.get(peerReplicaID);
        timer.purge();
        timer.schedule(timerTask, recalculationTimeInMillis);
    }

    /**
     * Stops the detector.
     * @return
     */
    @Override
    public boolean stop() {

        for (Timer timer : timers.values())
            timer.cancel();

        return stopped = true;
    }

    @Override
    public int getFailureTimeoutValue() {
        return recalculationTimeInMillis;
    }

    /**
     * Returns some fake samples.
     *
     * @return
     */
    private int[] getFakeSamples() {
        int[] timestampSamples = new int[11];
        timestampSamples[0] = 3;
        timestampSamples[1] = 5;
        timestampSamples[2] = 8;
        timestampSamples[3] = 12;
        timestampSamples[4] = 14;
        timestampSamples[5] = 15;
        timestampSamples[6] = 19;
        timestampSamples[7] = 20;
        timestampSamples[8] = 22;
        timestampSamples[9] = 25;
        timestampSamples[10] = 29;

        int[] delta = new int[10];
        for (int i=0; i<10; i++) {
            delta[i] = timestampSamples[i + 1] - timestampSamples[i];
        }

        return delta;
    }
}