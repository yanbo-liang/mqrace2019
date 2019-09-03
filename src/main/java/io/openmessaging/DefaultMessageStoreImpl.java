package io.openmessaging;

import io.openmessaging.core.MessagePut;
import io.openmessaging.core.MessageReadResult;
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

//    ConcurrentHashMap<Integer, AtomicLong> aPartitionMap = new ConcurrentHashMap<>();

    @Override
    void put(Message message) {

//        int i = Long.numberOfLeadingZeros(message.getA());
//
//
//        AtomicLong atomicLong = aPartitionMap.get(i);
//        if (atomicLong == null) {
//            atomicLong = new AtomicLong(0);
//            aPartitionMap.put(i,atomicLong);
//        }
//        atomicLong.incrementAndGet();
        try {
            MessagePut.put(message);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        try {
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!getStarted.get()) {
                if (getStarted.compareAndSet(false, true)) {

                    MessagePut.putEnd();
                    DirectBufferManager.freeWriteBuffer();
//                    MessageCache.buildCache();

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
            ByteBuffer bodyBuffer = MessageReader.readBody(tMin, tMax);
            LongBuffer longBuffer = PartitionIndex.getTArray(tMin, tMax);

            long[] tArray = longBuffer.array();

            List<Message> messageList = new ArrayList<>();
            for (int i = 0; i < longBuffer.limit(); i++) {
                long t = tArray[i];
                long a = aBuffer.getLong();
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    byte[] b = new byte[Constants.Body_Size];
                    bodyBuffer.get(b, 0, Constants.Body_Size);
                    messageList.add(new Message(a, t, b));
                } else {
                    bodyBuffer.position(bodyBuffer.position() + Constants.Body_Size);
                }
            }
            System.out.println("get time:\t" + (System.currentTimeMillis() - start));
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
            long start = System.currentTimeMillis();

            long sum = 0, count = 0;

            MessageReadResult readResult = MessageReader.readAFast(aMin, aMax, tMin, tMax);
            ByteBuffer aBuffer = readResult.buffer;
            System.out.println("read a:" + (System.currentTimeMillis() - start));

            while (aBuffer.hasRemaining()) {
                long a=0;
                if (aBuffer.position() >= readResult.mark) {
                    try {
                        a = readA(aBuffer);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    a = aBuffer.getLong();

                }
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }

            System.out.println("average:" + (System.currentTimeMillis() - start));
            return count == 0 ? 0 : sum / count;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
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