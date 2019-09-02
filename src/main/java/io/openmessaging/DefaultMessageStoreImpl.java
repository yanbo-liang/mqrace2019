package io.openmessaging;

import io.openmessaging.core.MessageCache;
import io.openmessaging.core.MessagePut;
import io.openmessaging.core.MessageReader;
import io.openmessaging.core.PartitionIndex;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMessageStoreImpl extends MessageStore {
    private AtomicBoolean getStarted = new AtomicBoolean(false);
    private volatile boolean readyForRead = false;
    private long initStart;

    public DefaultMessageStoreImpl() {
        initStart = System.currentTimeMillis();
    }

    ConcurrentHashMap<Integer, AtomicLong> map = new ConcurrentHashMap<>();

    @Override
    void put(Message message) {

        int i = Long.numberOfLeadingZeros(message.getA());


        AtomicLong atomicLong = map.get(i);
        if (atomicLong == null) {
            atomicLong = new AtomicLong(0);
            map.put(i,atomicLong);
        }
        atomicLong.incrementAndGet();
//        try {
//            MessagePut.put(message);
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        try {
            System.out.println(map);
            System.exit(1);
            long heapSize = Runtime.getRuntime().freeMemory();
            System.out.println("heapSize " + heapSize);
            System.out.println(System.currentTimeMillis() - initStart);
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!getStarted.get()) {
                if (getStarted.compareAndSet(false, true)) {

                    MessagePut.putEnd();
                    DirectBufferManager.freeWriteBuffer();
                    MessageCache.buildCache();

                    readyForRead = true;
                    synchronized (this) {
                        this.notifyAll();
                    }
                }
            }
            if (!readyForRead) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            long start = System.currentTimeMillis();
            ByteBuffer aBuffer = MessageReader.readA(tMin, tMax);
            ByteBuffer bodyBuffer = MessageReader.readBody(null, tMin, tMax);
            LongBuffer longBuffer = PartitionIndex.getTArray(tMin, tMax);
            long[] tArray = longBuffer.array();
            List<Message> messageList = new ArrayList<>();
            for (int i = 0; i < longBuffer.limit(); i++) {
                long t = tArray[i];
                long a = readA(aBuffer);
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    byte[] b = new byte[Constants.Body_Size];
                    bodyBuffer.get(b, 0, Constants.Body_Size);
                    messageList.add(new Message(a, t, b));
                } else {
                    bodyBuffer.position(bodyBuffer.position() + Constants.Body_Size);
                }
            }
            if (aBuffer instanceof DirectBuffer) {
                ((DirectBuffer) aBuffer).cleaner().clean();
            }
            if (bodyBuffer instanceof DirectBuffer) {
                ((DirectBuffer) bodyBuffer).cleaner().clean();
            }
            System.out.println("gt:\t" + (System.currentTimeMillis() - start));
            return messageList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        try {
            System.out.println("a " + aMin + " " + aMax + " " + tMin + " " + tMax);
            long startToal = System.currentTimeMillis();
            long start = System.currentTimeMillis();
            long sum = 0, count = 0;
            ByteBuffer aBuffer = MessageReader.readA(tMin, tMax);
            System.out.println("read a:" + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            LongBuffer longBuffer = PartitionIndex.getTArray(tMin, tMax);
            System.out.println("read t:" + (System.currentTimeMillis() - start));

            long[] tArray = longBuffer.array();
            for (int i = 0; i < longBuffer.limit(); i++) {
                long t = tArray[i];
                long a = readA(aBuffer);
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }
            if (aBuffer instanceof DirectBuffer) {
                ((DirectBuffer) aBuffer).cleaner().clean();
            }
            System.out.println("average:" + (System.currentTimeMillis() - startToal));
            return count == 0 ? 0 : sum / count;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    long readA(ByteBuffer buffer) {
        byte b1 = buffer.get();
        byte b2 = buffer.get();
        byte b3 = buffer.get();
        byte b4 = buffer.get();
        byte b5 = buffer.get();
        byte b6 = buffer.get();
        long a = ((b1 & 0xffL) << 40) |
                ((b2 & 0xffL) << 32) |
                ((b3 & 0xffL) << 24) |
                ((b4 & 0xffL) << 16) |
                ((b5 & 0xffL) << 8) |
                ((b6 & 0xffL));
        if (a != 0) {
            return a;
        } else {
            return buffer.getLong();
        }
    }
}