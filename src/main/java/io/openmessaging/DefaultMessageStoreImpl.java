package io.openmessaging;

import io.openmessaging.core.MessagePut;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultMessageStoreImpl extends MessageStore {
    private AtomicBoolean getStarted = new AtomicBoolean(false);
    private volatile boolean readyForRead = false;
    private long initStart;

    private byte[] a;
    private byte[] b;

    public DefaultMessageStoreImpl() {
        initStart = System.currentTimeMillis();
    }


    @Override
    void put(Message message) {
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
            long heapSize = Runtime.getRuntime().totalMemory();
            System.out.println("heapSize "+heapSize);
            System.out.println(System.currentTimeMillis() - initStart);
//            System.exit(1);
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!getStarted.get()) {
                if (getStarted.compareAndSet(false, true)) {

                    MessagePut.putEnd();
                    a=new byte[2000000000];
                    b=new byte[1000000000];

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
//            ByteBuffer buffer = DirectBufferManager.borrowBodyBuffer();
            ByteBuffer aBuffer = MessageReader.readA(null, tMin, tMax);
            ByteBuffer bodyBuffer = MessageReader.readBody(null, tMin, tMax);
            long[] tArray = PartitionIndex.getTArray(tMin, tMax);
            List<Message> messageList = new ArrayList<>();
            for (int i = 0; i < tArray.length; i++) {
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

//            while (buffer.hasRemaining()) {
//                int dataSize = Constants.Message_Size - 16;
//                long t = tArray[index++];
//                long a = buffer.getLong();
//                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
//                    byte[] b = new byte[dataSize];
//                    buffer.get(b, 0, dataSize);
//                    messageList.add(new Message(a, t, b));
//                } else {
//                    buffer.position(buffer.position() + dataSize);
//                }
//            }
//            DirectBufferManager.returnBodyBuffer(buffer);
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

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        try {

            System.out.println("a " + aMin + " " + aMax + " " + tMin + " " + tMax);
            long start = System.currentTimeMillis();
            long sum = 0, count = 0;

//            ByteBuffer buffer = DirectBufferManager.borrowBodyBuffer();
            ByteBuffer aBuffer = MessageReader.readA(null, tMin, tMax);
//            while (buffer.hasRemaining()) {
//                long a = buffer.getLong();
//                if (aMin <= a && a <= aMax) {
//                    sum += a;
//                    count += 1;
//                }
//            }


            long[] tArray = PartitionIndex.getTArray(tMin, tMax);
            for (int i = 0; i < tArray.length; i++) {
                long t = tArray[i];
                long a = readA(aBuffer);
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }


//            DirectBufferManager.returnBodyBuffer(buffer);
            if (aBuffer instanceof DirectBuffer) {
                ((DirectBuffer) aBuffer).cleaner().clean();

            }
            System.out.println("average:" + (System.currentTimeMillis() - start));
            return count == 0 ? 0 : sum / count;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}