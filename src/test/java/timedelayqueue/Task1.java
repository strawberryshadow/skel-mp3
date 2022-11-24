package timedelayqueue;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class Task1 {

    private static final int DELAY        = 40; // delay of 40 milliseconds
    private static final int MSG_LIFETIME = 80;

    @Test
    public void testBasicAddRetrieve_NoDelay() {
        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);
        UUID sndID     = UUID.randomUUID();
        UUID rcvID     = UUID.randomUUID();
        String msgText = "test";
        PubSubMessage msg1 = new PubSubMessage(sndID, rcvID, msgText);
        tdq.add(msg1);
        PubSubMessage msg2 = tdq.getNext();
        assertEquals(PubSubMessage.NO_MSG, msg2);
    }

    @Test
    public void testBasicAddRetrieve_Delay() {
        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);
        UUID sndID     = UUID.randomUUID();
        UUID rcvID     = UUID.randomUUID();
        String msgText = "test";
        PubSubMessage msg1 = new PubSubMessage(sndID, rcvID, msgText);
        tdq.add(msg1);
        try {
            Thread.sleep(2 * DELAY);
        }
        catch (InterruptedException ie) {
            // nothing to do but ...
            fail();
        }
        PubSubMessage msg2 = tdq.getNext();
        assertEquals(msg1, msg2);
    }

    @Test
    public void testTransientMsg_InTime() {
        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);

        UUID sndID     = UUID.randomUUID();
        UUID rcvID     = UUID.randomUUID();
        String msgText = "test";
        TransientPubSubMessage msg1 = new TransientPubSubMessage(sndID, rcvID, msgText, MSG_LIFETIME);
        PubSubMessage          msg2 = new PubSubMessage(sndID, rcvID, msgText);
        tdq.add(msg1);
        tdq.add(msg2);
        try {
            Thread.sleep(DELAY + 1);
        }
        catch (InterruptedException ie) {
            fail();
        }
        assertEquals(msg1, tdq.getNext());
        assertEquals(msg2, tdq.getNext());
    }

    @Test
    public void testTransientMsg_Late() {
        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);

        UUID sndID     = UUID.randomUUID();
        UUID rcvID     = UUID.randomUUID();
        String msgText = "test";
        TransientPubSubMessage msg1 = new TransientPubSubMessage(sndID, rcvID, msgText, MSG_LIFETIME);
        PubSubMessage          msg2 = new PubSubMessage(sndID, rcvID, msgText);
        tdq.add(msg1);
        tdq.add(msg2);
        try {
            Thread.sleep(MSG_LIFETIME + 1);
        }
        catch (InterruptedException ie) {
            fail();
        }
        assertEquals(msg2, tdq.getNext()); // msg1 would have expired
    }

    @Test
    public void testMsgCount() {
        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);

        final int NUM_MSGS = 10;
        for (int i = 0; i < NUM_MSGS; i++) {
            UUID sndID        = UUID.randomUUID();
            UUID rcvID        = UUID.randomUUID();
            String msgText    = "test";
            PubSubMessage msg = new PubSubMessage(sndID, rcvID, msgText);
            tdq.add(msg);
        }

        try {
            Thread.sleep(2 * DELAY);
        }
        catch (InterruptedException ie) {
            fail();
        }

        for (int i = 0; i < NUM_MSGS; i++) {
            tdq.getNext();
        }

        assertEquals(NUM_MSGS, tdq.getTotalMsgCount());
    }

}
