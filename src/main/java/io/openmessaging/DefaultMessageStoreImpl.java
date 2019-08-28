package io.openmessaging;

import io.openmessaging.unsafe.UnsafePut;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultMessageStoreImpl extends MessageStore {
    private AtomicBoolean getStarted = new AtomicBoolean(false);
    private volatile boolean readyForRead = false;
    private long initStart;

    public DefaultMessageStoreImpl() {
        initStart = System.currentTimeMillis();
    }
int a=0;
    @Override
    void put(Message message) {
        System.out.println(message.getT() + "\t" + message.getA());
        a++;
        if (a>500000){
            System.exit(1);
        }
//        try {
//            UnsafePut.put(message);
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        try {
            System.out.println(System.currentTimeMillis() - initStart);
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!getStarted.get()) {
                if (getStarted.compareAndSet(false, true)) {
                    UnsafePut.putEnd();
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
            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            MessageReader.read(buffer, tMin, tMax);
            buffer.flip();
            long[] tArray = PartitionIndex.getTArray(tMin, tMax);
            int index = 0;
            List<Message> messageList = new ArrayList<>();
            while (buffer.hasRemaining()) {
                int dataSize = Constants.Message_Size - 16;
                long t = tArray[index++];
                long a = buffer.getLong();
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    byte[] b = new byte[dataSize];
                    buffer.get(b, 0, dataSize);
                    messageList.add(new Message(a, t, b));
                } else {
                    buffer.position(buffer.position() + dataSize);
                }
            }
            DirectBufferManager.returnBuffer(buffer);
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
            long start = System.currentTimeMillis();
            long sum = 0, count = 0;

            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            MessageReader.fastRead(buffer, tMin, tMax);
            buffer.flip();
            while (buffer.hasRemaining()) {
                long a = buffer.getLong();
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }
            DirectBufferManager.returnBuffer(buffer);
            System.out.println("average:" + (System.currentTimeMillis() - start));
            return count == 0 ? 0 : sum / count;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}