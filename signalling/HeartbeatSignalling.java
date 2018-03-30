package siemens.db.adapt.FailureDetection.signalling;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.protocol.Generic;

public class HeartbeatSignalling implements SignallingStrategy {

    private long nextMessageId;
    private FailureDetectorContext failureDetectorContext;

    /**
     *
     */
    public HeartbeatSignalling() {
        nextMessageId = 0;
    }

    /**
     *
     * @param failureCheckMessage
     * @return
     */
    @Override
    public Failuredetector.FailureCheck.Builder addSignallingHeader(Failuredetector.FailureCheck.Builder failureCheckMessage) {

        nextMessageId ++;

        return failureCheckMessage
                .setMessageId(nextMessageId)
                .setSignallingStrategy(Failuredetector.FailureCheck.SignallingStrategy.HEARTBEAT)
                .setMessageType(Failuredetector.FailureCheck.MessageType.PING);
    }

    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheckMessage) {
        // nothing to do here for HeartBeat signalling.
    }

    @Override
    public void setFailureDetectorContext(FailureDetectorContext failureDetectorContext) {
        this.failureDetectorContext = failureDetectorContext;
    }

    /**
     *
     * @return
     */
    @Override
    public Failuredetector.FailureCheck.SignallingStrategy getSignallingMethod() {

        return Failuredetector.FailureCheck.SignallingStrategy.HEARTBEAT;
    }


}
