package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    private MessageWriter writer = new MessageWriter();
    private MessageReader reader = new MessageReader();

    private AtomicInteger messageCount = new AtomicInteger(0);

    int messageBatchSize = Constants.Message_Batch_Size;
    int messageSize = Constants.Message_Size;

    private volatile byte[] messageBuffer = new byte[messageBatchSize * messageSize];

    private AtomicInteger concurrencyCounter = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);
    volatile boolean wait = true;

    private void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        ByteUtils.putLong(messageBuffer, message.getT(), startIndex);
        ByteUtils.putLong(messageBuffer, message.getA(), startIndex + 8);
        System.arraycopy(message.getBody(), 0, messageBuffer, startIndex + 16, messageSize - 16);
    }

    @Override
    void put(Message message) {
        try {
            concurrencyCounter.incrementAndGet();
            int count = messageCount.getAndIncrement();
            if (count < messageBatchSize - 1) {
                messageToBuffer(count, message);
                concurrencyCounter.decrementAndGet();

            } else if (count == messageBatchSize - 1) {
                messageToBuffer(count, message);
                concurrencyCounter.decrementAndGet();

                while (concurrencyCounter.get() != 0) {
                }

                writer.write(new MessageWriterTask(messageBuffer));
                messageBuffer = new byte[messageBatchSize * messageSize];
                messageCount.getAndUpdate(x -> 0);
                synchronized (this) {
                    this.notifyAll();
                }
            } else if (count > messageBatchSize - 1) {
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
//
//        _messages[_index++] = message;
//
//        if (_index == length) {
//            long start = System.currentTimeMillis();
//            Arrays.parallelSort(_messages, new MessageComparator());
//            System.out.println(System.currentTimeMillis() - start);
//            int splitIndex = 100000;
//            while (_messages[splitIndex - 1].getT() == _messages[splitIndex].getT()) {
//                splitIndex += 1;
//            }
//            Message[] pickedMessages = Arrays.copyOf(_messages, splitIndex);
//            _messages = Arrays.copyOfRange(_messages, splitIndex, splitIndex + length);
//            _index = length - splitIndex;
//            writer.write(new MessageWriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT()));
//        }
    }

    private volatile boolean readyForRead = false;

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);

        if (!init.get()) {
            if (init.compareAndSet(false, true)) {
                writer.flushAndShutDown(messageBuffer, messageCount.get() * Constants.Message_Size);
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

        buffer.flip();

        byte[] aa = new byte[16];

        while (buffer.position() < buffer.limit()) {

            buffer.get(aa, 0, 16);
            int dataSize = Constants.Message_Size - 16;
            long t = ByteUtils.getLong(aa, 0);
            long a = ByteUtils.getLong(aa, 8);
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

//        try {
//            if (!init.get()) {
//                if (init.compareAndSet(false, true)) {
//                    if (_index != 0) {
//                        _messages = Arrays.copyOf(_messages, _index);
//
//                        Arrays.parallelSort(_messages, new MessageComparator());
//
//                        int startIndex = 0;
//                        int splitIndex = 100000;
//
//                        List<MessageWriterTask> writerTasks = new ArrayList<>();
//                        while (splitIndex < _index) {
//                            while (_messages[splitIndex - 1].getT() == _messages[splitIndex].getT()) {
//                                if (splitIndex + 1 < _index) {
//                                    splitIndex += 1;
//                                }
//                            }
//
//                            Message[] pickedMessages = Arrays.copyOfRange(_messages, startIndex, splitIndex);
//                            startIndex = splitIndex;
//                            splitIndex += 100000;
//                            MessageWriterTask writerTask = new MessageWriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT());
//                            writer.write(writerTask);
//                            writerTasks.add(writerTask);
//                        }
//                        System.out.println(splitIndex - 100000 + " " + _index);
//                        Message[] pickedMessages = Arrays.copyOfRange(_messages, splitIndex - 100000, _index);
//                        MessageWriterTask writerTask = new MessageWriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT());
//                        writer.write(writerTask);
//                        writerTasks.add(writerTask);
//
//                        while (true) {
//                            if (check(writerTasks)) {
//                                break;
//                            }
//                        }
//                        _index = 0;
//                        writer.executorService.shutdownNow();
//                        Set<Map.Entry<Long, Path>> entries = MessageReader.lowerMap.entrySet();
//                        for (Map.Entry<Long, Path> entriy : entries) {
//                            System.out.println(entriy.getValue());
//                        }
//                    }
//
//                    wait = false;
//                }
//            }
//            while (wait) {
//
//            }
//            long start = System.currentTimeMillis();
//            List<Message> aa = new ArrayList<>(100000);
//            List<ByteBuffer> buffers = MessageReader.read(tMin, tMax);
//            for (ByteBuffer buffer : buffers) {
//                buffer.flip();
//                while (buffer.position() < buffer.limit()) {
//
//                    long t = buffer.getLong();
//                    long a = buffer.getLong();
//                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
//                        byte[] b = new byte[34];
//                        buffer.get(b, 0, 34);
//                        aa.add(new Message(a, t, b));
//                    } else {
//                        buffer.position(buffer.position() + 34);
//
//                    }
//                }
//                ((DirectBuffer) buffer).cleaner().clean();
//            }
//            System.out.println(System.currentTimeMillis() - start);
//            return aa;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        System.out.println("a " + aMin + " " + aMax + " " + tMin + " " + tMax);
        long start = System.currentTimeMillis();

        ByteBuffer buffer = reader.read(tMin, tMax);

        buffer.flip();

        byte[] aa = new byte[16];

        long total = 0;
        long count = 0;
        while (buffer.position() < buffer.limit()) {

            buffer.get(aa, 0, 16);
            int dataSize = Constants.Message_Size - 16;

            long t = ByteUtils.getLong(aa, 0);
            long a = ByteUtils.getLong(aa, 8);
            if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                count++;
                total += a;
                buffer.position(buffer.position() + dataSize);
            } else {
                buffer.position(buffer.position() + dataSize);
            }
        }
        DirectBufferManager.returnBuffer(buffer);
        System.out.println("average:" + (System.currentTimeMillis() - start));
        return count == 0 ? 0 : total / count;

    }
}
