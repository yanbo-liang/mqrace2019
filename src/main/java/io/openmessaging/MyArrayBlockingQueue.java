package io.openmessaging;


import java.util.concurrent.atomic.AtomicInteger;

public class MyArrayBlockingQueue {

    final long[] items;

    /**
     * items index for next take, poll, peek or remove
     */
    int takeIndex;

    /**
     * items index for next put, offer, or add
     */
    int putIndex;

    /**
     * Number of elements in the queue
     */
    AtomicInteger count = new AtomicInteger(0);

    /*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */

    private static class Flag {

    }

    /**
     * Condition for waiting takes
     */
    private final Flag notEmpty = new Flag();

    /**
     * Condition for waiting puts
     */
    private final Flag notFull = new Flag();

    private final Flag notHalf = new Flag();


    /**
     * Returns item at index i.
     */
    @SuppressWarnings("unchecked")
    final long itemAt(int i) {
        return items[i];
    }

    public MyArrayBlockingQueue(int capacity) {
        if (capacity <= 0)
            throw new IllegalArgumentException();
        this.items = new long[capacity];
    }

    private void enqueue(long x) {
        final long[] items = this.items;
        items[putIndex] = x;
        if (++putIndex == items.length)
            putIndex = 0;
        int i = count.incrementAndGet();
        if (i >= items.length / 2) {
            synchronized (notHalf) {
                notHalf.notifyAll();
            }
        }
        synchronized (notEmpty) {
            notEmpty.notifyAll();
        }
    }

    private long dequeue() {
        final long[] items = this.items;
        @SuppressWarnings("unchecked")
        long x = items[takeIndex];
//        items[takeIndex] = null;
        if (++takeIndex == items.length)
            takeIndex = 0;
        count.decrementAndGet();
        synchronized (notFull) {
            notFull.notifyAll();
        }
        return x;
    }


    public void put(long e) throws InterruptedException {
        while (count.get() == items.length) {
            synchronized (notFull) {
                notFull.wait();
            }
        }
        enqueue(e);
    }

    public long take() throws InterruptedException {
        while (count.get() == 0) {
            synchronized (notEmpty) {
                notEmpty.wait();
            }
        }
        return dequeue();
    }

    public long poll() {
        return (count.get() == 0) ? -1 : dequeue();
    }

    public long peek() throws InterruptedException {
        while (count.get() < items.length / 2) {
            synchronized (notHalf) {
                notHalf.wait();
            }
        }
        return (count.get() == 0) ? -1 : itemAt(takeIndex); // null when queue is empty
    }
}
