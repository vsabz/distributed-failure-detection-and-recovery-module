package siemens.db.adapt.FailureDetection;

import siemens.db.adapt.FailureDetection.consensus.ConsensusStrategy;
import siemens.db.adapt.FailureDetection.dissemination.DisseminationStrategy;
import siemens.db.adapt.FailureDetection.failuretrigger.AccrualFailureDetector;
import siemens.db.adapt.FailureDetection.failuretrigger.FailureTriggerOnTimer;
import siemens.db.adapt.FailureDetection.failuretrigger.LocalFailureTriggerStrategy;
import siemens.db.adapt.FailureDetection.signalling.SignallingStrategy;

public class FailureDetectorContextBuilder {

    // Variables to hold Strategies
    private SignallingStrategy signallingStrategy;
    private DisseminationStrategy disseminationStrategy;
    private ConsensusStrategy consensusStrategy;
    private LocalFailureTriggerStrategy localFailureTriggerStrategy;
    private int failureTimeout;
    private int disseminationTime;

    /**
     * Returns Signalling Strategy object.
     *
     * @return SignallingStrategy
     */
    public SignallingStrategy getSignallingStrategy() {
        return signallingStrategy;
    }

    /**
     * Sets Signalling Strategy.
     *
     * @param signallingStrategy
     * @return FailureDetectorContext Builder object.
     */
    public FailureDetectorContextBuilder setSignallingStrategy(SignallingStrategy signallingStrategy) {
        this.signallingStrategy = signallingStrategy;

        return this;
    }

    /**
     * Returns dissemination interval.
     *
     * @return time in ms
     */
    public int getDisseminationTime() {
        return disseminationTime;
    }

    /**
     * Sets disseminaion interval
     *
     * @param disseminationTimeInMilliSeconds time in ms
     */
    public FailureDetectorContextBuilder setDisseminationTime(int disseminationTimeInMilliSeconds) {

        this.disseminationTime = disseminationTimeInMilliSeconds;

        return this;
    }

    /**
     * Returns Dissemination Strategy object.
     *
     * @return DisseminationStrategy
     */
    public DisseminationStrategy getDisseminationStrategy() {
        return disseminationStrategy;
    }

    /**
     * Sets Signalling Strategy.
     *
     * @param disseminationStrategy
     * @return FailureDetectorContext Builder object.
     */
    public FailureDetectorContextBuilder setDisseminationStrategy(DisseminationStrategy disseminationStrategy) {
        this.disseminationStrategy = disseminationStrategy;

        return this;
    }

    /**
     * Returns Consensus Strategy object.
     *
     * @return ConsensusStrategy
     */
    public ConsensusStrategy getConsensusStrategy() {
        return consensusStrategy;
    }

    /**
     * Sets Consensus Strategy.
     *
     * @param consensusStrategy
     * @return FailureDetectorContext Builder object.
     */
    public FailureDetectorContextBuilder setConsensusStrategy(ConsensusStrategy consensusStrategy) {
        this.consensusStrategy = consensusStrategy;

        return this;
    }

    /**
     * Returns failure trigger strategy used.
     *
     * @return
     */
    public LocalFailureTriggerStrategy getLocalFailureTriggerStrategy() {
        return localFailureTriggerStrategy;
    }

    /**
     * Sets failure trigger strategy used.
     *
     * @param localFailureTriggerStrategy
     * @return
     */
    public FailureDetectorContextBuilder setLocalFailureTriggerStrategy(LocalFailureTriggerStrategy localFailureTriggerStrategy) {
        this.localFailureTriggerStrategy = localFailureTriggerStrategy;

        return this;
    }

    /**
     * Returns timeout value for failure detection.
     *
     * @return
     */
    public int getFailureTimeout() {
        return failureTimeout;
    }

    /**
     * Sets timeout value for failure detection.
     *
     * @param failureTimeout timeout value
     * @return builder object
     */
    public FailureDetectorContextBuilder setFailureTimeout(int failureTimeout) {
        this.failureTimeout = failureTimeout;

        return this;
    }

    /**
     * Checks if necessary parameters are set
     * and creates FailureDetectorContext object.
     *
     * @return FailureDetectorContext object
     */
    public FailureDetectorContext build() {

        if (signallingStrategy == null)
            throw new IllegalStateException("Signalling strategy must be set !");

        if (disseminationStrategy == null)
            throw new IllegalStateException("Dissemination strategy must be set !");

        if (!(localFailureTriggerStrategy instanceof AccrualFailureDetector) && failureTimeout == 0)
            throw new IllegalStateException("Either AccrualFailureDetector should be created as " +
                    "LocalFailureTriggerStrategy, or failure timeout value must be set for FailureTriggerOnTimer" +
                    "to be used as default strategy.");

        else if (localFailureTriggerStrategy == null)
            localFailureTriggerStrategy = new FailureTriggerOnTimer(failureTimeout);

        return new FailureDetectorContext(this);
    }
}
