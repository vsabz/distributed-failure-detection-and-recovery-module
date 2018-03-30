package siemens.db.adapt.FailureDetection.signalling;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.protocol.Generic;

public interface SignallingStrategy {

    /**
     * Returns a message to be disseminated.
     *
     * @return message of type Generic.Update
     */
    Failuredetector.FailureCheck.Builder addSignallingHeader(Failuredetector.FailureCheck.Builder failureCheckMessage);

    void handleReceivedMessage(Failuredetector.FailureCheck failureCheckMessage);

    void setFailureDetectorContext(FailureDetectorContext failureDetectorContext);

    Failuredetector.FailureCheck.SignallingStrategy getSignallingMethod();
}
