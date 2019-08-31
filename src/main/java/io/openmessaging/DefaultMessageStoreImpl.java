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
            System.out.println(System.currentTimeMillis() - initStart);
//            System.exit(1);
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!getStarted.get()) {
                if (getStarted.compareAndSet(false, true)) {
                    MessagePut.putEnd();
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
                long a = aBuffer.getLong();
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
            if (aBuffer instanceof DirectBuffer){
                ((DirectBuffer) aBuffer).cleaner().clean();
            }
            if (bodyBuffer instanceof DirectBuffer){
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
            long start = System.currentTimeMillis();
            long sum = 0, count = 0;

//            ByteBuffer buffer = DirectBufferManager.borrowBodyBuffer();
            ByteBuffer buffer = MessageReader.fastRead(null, tMin, tMax);
            while (buffer.hasRemaining()) {
                long a = buffer.getLong();
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }
//            DirectBufferManager.returnBodyBuffer(buffer);
            if (buffer instanceof DirectBuffer){
                ((DirectBuffer) buffer).cleaner().clean();

            }
            System.out.println("average:" + (System.currentTimeMillis() - start));
            return count == 0 ? 0 : sum / count;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}