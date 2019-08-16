package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    private MessageWriter writer = new MessageWriter();
    private MessageReader reader = new MessageReader();

    private AtomicInteger messageCount = new AtomicInteger(0);

    private int batchSize = Constants.Message_Batch_Size;
    private int messageSize = Constants.Message_Size;

    private volatile ByteBuffer buffer = ByteBuffer.allocate(batchSize * messageSize);

    private AtomicInteger concurrencyCounter = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);
    volatile boolean wait = true;

    private volatile boolean readyForRead = false;

    private void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        buffer.putLong(startIndex, message.getT());
        buffer.putLong(startIndex + 8, message.getA());
        for (int i = 0; i < messageSize - 16; i++) {
            buffer.put(startIndex + 16 + i, message.getBody()[i]);
        }
    }

    @Override
    void put(Message message) {
        try {
            concurrencyCounter.incrementAndGet();
            int count = messageCount.getAndIncrement();
            if (count < batchSize - 1) {
                messageToBuffer(count, message);
                concurrencyCounter.decrementAndGet();

            } else if (count == batchSize - 1) {
                messageToBuffer(count, message);
                concurrencyCounter.decrementAndGet();

                while (concurrencyCounter.get() != 0) {
                }

                writer.write(new MessageWriterTask(buffer));
                buffer = ByteBuffer.allocate(batchSize * messageSize);
                messageCount.getAndUpdate(x -> 0);
                synchronized (this) {
                    this.notifyAll();
                }
            } else if (count > batchSize - 1) {
                concurrencyCounter.decrementAndGet();

                synchronized (this) {
                    this.wait();
                }

                concurrencyCounter.incrementAndGet();
                count = messageCount.getAndIncrement();
                messageToBuffer(count, message);
                concurrencyCounter.decrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);

        if (!init.get()) {
            if (init.compareAndSet(false, true)) {
                writer.flushAndShutDown(buffer, messageCount.get() * Constants.Message_Size);
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

        ByteBuffer buffer = reader.read(tMin, tMax);
        List<Message> messageList = new ArrayList<>();
        if (buffer == null) {
            return messageList;
        }
        buffer.flip();


        while (buffer.position() < buffer.limit()) {

            int dataSize = Constants.Message_Size - 16;
            long t = buffer.getLong();
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
        System.out.println("average:" + (System.currentTimeMillis() - start));

        return messageList;
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        System.out.println("a " + aMin + " " + aMax + " " + tMin + " " + tMax);
        long start = System.currentTimeMillis();
        long total = 0;
        int count = 0;
        List<PartitionIndex.PartitionInfo> missedPartitionInfo = new ArrayList<>();
        NavigableMap<Integer, PartitionIndex.PartitionInfo> partitionMap = PartitionIndex.bc(aMin, aMax, tMin, tMax);
        for (Map.Entry<Integer, PartitionIndex.PartitionInfo> entry : partitionMap.entrySet()) {
            Integer partitionIndex = entry.getKey();
            long tLow = partitionIndex * 2000;
            long tHigh = tLow + 1999;
            PartitionIndex.PartitionInfo partitionInfo = entry.getValue();
            if (tMin <= tLow && tHigh <= tMax) {
                if (aMin <= partitionInfo.low && partitionInfo.high <= aMax) {
                    total += partitionInfo.sum;
                    count += partitionInfo.count;
                    continue;
                }
            }
            missedPartitionInfo.add(partitionInfo);
        }


        ByteBuffer buffer = reader.readMissedPartition(missedPartitionInfo);

        buffer.flip();


        while (buffer.position() < buffer.limit()) {


            long t = buffer.getLong();
            long a = buffer.getLong();
            if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                count++;
                total += a;
            }
            int dataSize = Constants.Message_Size - 16;

            buffer.position(buffer.position() + dataSize);

        }
        DirectBufferManager.returnBuffer(buffer);
        System.out.println("average:" + (System.currentTimeMillis() - start));

        return count == 0 ? 0 : total / count;

    }
}
