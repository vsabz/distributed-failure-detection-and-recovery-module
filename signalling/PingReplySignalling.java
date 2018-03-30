package siemens.db.adapt.FailureDetection.signalling;

import siemens.db.adapt.FailureDetection.FailureDetectorContext;
import siemens.db.adapt.protocol.Failuredetector;
import siemens.db.adapt.protocol.Generic;

import java.util.HashMap;

public class PingReplySignalling implements SignallingStrategy {


    // keep history of sent REPLY messages < ClientID, < MessageID, timestamp >>
    private HashMap<String, HashMap<Long, Long>> sentReplyMessages;
    // keep history of received REPLY messages < ClientID, < MessageID, timestamp >>
    private HashMap<String, HashMap<Long, Long>> receivedReplyMessages;

    private FailureDetectorContext failureDetectorContext;
    private long nextMessageId;


    /**
     * Constructor initiates variables.
     */
    public PingReplySignalling() {
        nextMessageId = 0;
        sentReplyMessages = new HashMap<>();
        receivedReplyMessages = new HashMap<>();
    }

    /**
     * Adds signalling header to given message.
     *
     * @param failureCheckMessage
     * @return
     */
    @Override
    public Failuredetector.FailureCheck.Builder addSignallingHeader(Failuredetector.FailureCheck.Builder failureCheckMessage) {
        nextMessageId ++;

        return failureCheckMessage.setMessageId(nextMessageId)
                .setSignallingStrategy(Failuredetector.FailureCheck.SignallingStrategy.PINGREPLY)
                .setMessageType(Failuredetector.FailureCheck.MessageType.PING);
    }

    /**
     * This method handles incoming message from other replicas.
     *
     * If it is a PING message, it triggers a REPLY message to be sent.
     *
     * @param failureCheckMessage received message from other replicas.
     */
    @Override
    public void handleReceivedMessage(Failuredetector.FailureCheck failureCheckMessage) {
        // if it is a PING message
        if (failureCheckMessage.getMessageType().equals(Failuredetector.FailureCheck.MessageType.PING)) {

            Failuredetector.FailureCheck.Builder failureReply = Failuredetector.FailureCheck.newBuilder()
                    .setMessageId(failureCheckMessage.getMessageId())
                    .setDestinationReplicaId(failureCheckMessage.getReplicaId())
                    .setSignallingStrategy(Failuredetector.FailureCheck.SignallingStrategy.PINGREPLY)
                    .setMessageType(Failuredetector.FailureCheck.MessageType.REPLY);

            // trigger response
            failureDetectorContext.triggerSignallingReply(failureReply);

            // add response to history
            if (sentReplyMessages.get(failureCheckMessage.getReplicaId())== null)
                sentReplyMessages.put(failureCheckMessage.getReplicaId(), new HashMap<>());
            sentReplyMessages.get(failureCheckMessage.getReplicaId())
                    .put(failureReply.getMessageId(), System.nanoTime());
        }

        // if it is a REPLY message
        else if (failureCheckMessage.getMessageType().equals(Failuredetector.FailureCheck.MessageType.REPLY)) {
            // add to history
            // TODO intiate hashmaps during before this point
            if (receivedReplyMessages.get(failureCheckMessage.getReplicaId())== null)
                receivedReplyMessages.put(failureCheckMessage.getReplicaId(), new HashMap<>());
            receivedReplyMessages.get(failureCheckMessage.getReplicaId())
                    .put(failureCheckMessage.getMessageId(), System.nanoTime());
        }
    }

    /**
     * Sets FailureDetectionContext object.
     *
     * @param failureDetectorContext
     */
    @Override
    public void setFailureDetectorContext(FailureDetectorContext failureDetectorContext) {
        this.failureDetectorContext = failureDetectorContext;
    }

    /**
     * Returns signalling method used.
     *
     * @return
     */
    @Override
    public Failuredetector.FailureCheck.SignallingStrategy getSignallingMethod() {
        return Failuredetector.FailureCheck.SignallingStrategy.PINGREPLY;
    }

}
