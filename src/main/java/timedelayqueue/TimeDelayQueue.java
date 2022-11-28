package timedelayqueue;

import javax.crypto.interfaces.PBEKey;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.StreamSupport;


// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TimeDelayQueue {
    //things that need to be synchronized
    private PriorityQueue<PubSubMessage> queue;
    private Map<Timestamp, Integer> operationhistory;
    //todo: time reference

    private int delay;
    Comparator byTimestamp;

    // a comparator to sort messages
    private class PubSubMessageComparator implements Comparator<PubSubMessage> {
        public int compare(PubSubMessage msg1, PubSubMessage msg2) {
            return msg1.getTimestamp().compareTo(msg2.getTimestamp());
        }
    }

    /**
     * Create a new TimeDelayQueue
     *
     * @param delay the delay, in milliseconds, that the queue can tolerate, >= 0
     */
    public TimeDelayQueue(int delay) {
        byTimestamp = new PubSubMessageComparator();
        queue = new PriorityQueue<>(byTimestamp);
        this.delay = delay;
        operationhistory = new TreeMap<>();
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public boolean add(PubSubMessage msg) {
        synchronized (TimeDelayQueue.class) {
            if (!contains(msg)) {
                queue.add(msg);
                Timestamp now = now();
                if (operationhistory.containsKey(now)) {
                    operationhistory.replace(now, operationhistory.get(now) + 1);
                } else {
                    operationhistory.put(now(), 1);
                }

                return true;
            }
            return false;
        }
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     *
     * @return
     */
    public long getTotalMsgCount() {
        synchronized (TimeDelayQueue.class) {
            try {
                return operationhistory.values().stream().filter(x -> x > 0).reduce(Integer::sum).get();
            } catch (NoSuchElementException nse) {
                return 0;
            }
        }
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
    public PubSubMessage getNext() {
        synchronized (TimeDelayQueue.class) {
            removeExpired();
        }
        if (withinDelayRange(queue.peek())) {
            if (operationhistory.containsKey(now())) {
                operationhistory.replace(now(), operationhistory.get(now()) - 1);
            } else {
                operationhistory.put(now(), -1);
            }
            return queue.poll();
        }
        return PubSubMessage.NO_MSG;

    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {
        int maxOps = 0;
        synchronized (TimeDelayQueue.class) {
            for (Timestamp start : operationhistory.keySet()) {
                int currentOps = 0;
                for (Timestamp t : operationhistory.keySet()) {

                    if (Math.abs(start.getTime() - t.getTime()) <= timeWindow) {
                        currentOps += Math.abs(operationhistory.get(t));
                        System.out.println("pass:"+(start.getTime()-t.getTime()));
                    } else {
                        System.out.println(start.getTime()-t.getTime());
                    }
                }
                if (currentOps > maxOps) {
                    maxOps = currentOps;
                }
            }
        }

        return maxOps;
    }

    //////////////

    private boolean contains(PubSubMessage msg) {
        synchronized (TimeDelayQueue.class) {
            for (PubSubMessage ps : queue) {
                if (ps.getId().equals(msg.getId())) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean withinDelayRange(PubSubMessage msg) {
        return now().getTime() - msg.getTimestamp().getTime() > delay;
        //todo find out if the units match; i think they do?
    }

    private boolean expired(PubSubMessage msg) {
        synchronized (TimeDelayQueue.class) {
            if (msg.isTransient() && contains(msg)) {
                TransientPubSubMessage transientmsg = (TransientPubSubMessage) msg;
                boolean inRange = transientmsg.getLifetime() >=
                        now().getTime() - (transientmsg.getTimestamp().getTime());
                if (!inRange) {
                    queue.remove(msg);
                }
                return inRange;
            } else {
                return false;
            }
        }

    }

    private void removeExpired() {
        synchronized (TimeDelayQueue.class) {
            for (PubSubMessage ps : queue) {
                expired(ps);
            }
        }

    }

    private Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }
}
