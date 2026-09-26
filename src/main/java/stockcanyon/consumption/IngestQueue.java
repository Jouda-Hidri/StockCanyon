package stockcanyon.consumption;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import stockcanyon.Quote;

/**
 * The bounded buffer between the socket and the database writer, and the back-pressure that
 * keeps it bounded.
 *
 * <p>Two watermarks with a gap between them:
 *
 * <pre>
 *   depth >= high  ->  PAUSED: the socket is closed, the exchange holds the backlog
 *   depth <= low   ->  RESUMED: the socket may reopen, from the stored checkpoint
 * </pre>
 *
 * <p>The socket is closed rather than merely left unread, because the exchange's replay log is
 * the one buffer that is not in our heap: it survives a crash here, and it is already addressable
 * by the checkpoint. Holding a connection open while not reading it would instead park the backlog
 * in two TCP windows and, eventually, in the exchange's per-session send queue.
 *
 * <p>Nothing is ever dropped. Frames already in flight when the high mark trips land in the space
 * between {@code high} and {@code capacity}; if even that fills, {@link #put} blocks the socket
 * thread as a last resort.
 */
public class IngestQueue {

    private final ArrayBlockingQueue<Quote> queue;
    private final int capacity;
    private final int high;
    private final int low;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition resumed = lock.newCondition();
    private volatile boolean paused;
    private final AtomicLong highWatermarkTrips = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong takes = new AtomicLong();

    public IngestQueue(int capacity, int high, int low) {
        if (!(0 <= low && low < high && high < capacity)) {
            throw new IllegalArgumentException(
                    "need 0 <= low < high < capacity, got " + low + " / " + high + " / " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.capacity = capacity;
        this.high = high;
        this.low = low;
    }

    /**
     * Enqueues a quote, blocking only if the queue is completely full.
     *
     * @return true if this quote tripped the high watermark, meaning the caller must close the socket
     */
    public boolean put(Quote quote) throws InterruptedException {
        queue.put(quote);
        puts.incrementAndGet();
        if (paused || queue.size() < high) {
            return false;
        }
        lock.lock();
        try {
            if (paused) {
                return false;
            }
            paused = true;
            highWatermarkTrips.incrementAndGet();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves up to {@code max} quotes into {@code sink}, waiting at most {@code wait} for the first
     * batch to fill. Returns early once {@code max} is reached.
     */
    public void drain(List<Quote> sink, int max, Duration wait) throws InterruptedException {
        int before = sink.size();
        try {
            drainInto(sink, max, wait);
        } finally {
            takes.addAndGet(sink.size() - before);
        }
    }

    private void drainInto(List<Quote> sink, int max, Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        while (sink.size() < max) {
            queue.drainTo(sink, max - sink.size());
            long remaining = deadline - System.nanoTime();
            if (sink.size() >= max || remaining <= 0) {
                return;
            }
            Quote next = queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (next == null) {
                return;
            }
            sink.add(next);
        }
    }

    /** Called by the writer after each commit: lifts the pause once the backlog is down to low. */
    public void afterDrain() {
        if (!paused || queue.size() > low) {
            return;
        }
        lock.lock();
        try {
            if (paused && queue.size() <= low) {
                paused = false;
                resumed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Blocks the connection supervisor while paused. */
    public void awaitResumed() throws InterruptedException {
        if (!paused) {
            return;
        }
        lock.lock();
        try {
            while (paused) {
                resumed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public int depth() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public boolean isPaused() {
        return paused;
    }

    /** Quotes ever enqueued. With {@link #takenCount()}, tells whether the writer has got past a point. */
    public long putCount() {
        return puts.get();
    }

    /** Quotes ever handed to the writer. */
    public long takenCount() {
        return takes.get();
    }

    public long highWatermarkTrips() {
        return highWatermarkTrips.get();
    }
}
