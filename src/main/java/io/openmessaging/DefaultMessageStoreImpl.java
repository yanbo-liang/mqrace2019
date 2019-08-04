package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultMessageStoreImpl extends MessageStore {
    Writer writer = new Writer();
    int length = 5000000;
    Message[] messages = new Message[length];
    int index = 0;

    @Override
    synchronized void put(Message message) {
        messages[index++] = message;

        if (index == length) {
            long start = System.currentTimeMillis();
            Arrays.parallelSort(messages, new MessageComparator());
            System.out.println(System.currentTimeMillis() - start);
            int splitIndex = 200000;
            while (messages[splitIndex - 1].getT() == messages[splitIndex].getT()) {
                splitIndex += 1;
            }
            Message[] pickedMessages = Arrays.copyOf(messages, splitIndex);
            messages = Arrays.copyOfRange(messages, splitIndex, splitIndex + length);
            index = length - splitIndex;
            writer.write(new WriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT()));
        }
    }

    AtomicBoolean init = new AtomicBoolean(false);
    volatile boolean wait = true;

    @Override
     List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        try {
            if (!init.get()) {
                if (init.compareAndSet(false, true)) {
                    if (index != 0) {
                        messages = Arrays.copyOf(messages, index);

                        Arrays.parallelSort(messages, new MessageComparator());

                        int startIndex = 0;
                        int splitIndex = 200000;

                        List<WriterTask> writerTasks = new ArrayList<>();
                        while (splitIndex < index) {
                            while (messages[splitIndex - 1].getT() == messages[splitIndex].getT()) {
                                if (splitIndex + 1 < index) {
                                    splitIndex += 1;
                                }
                            }

                            Message[] pickedMessages = Arrays.copyOfRange(messages, startIndex, splitIndex);
                            startIndex = splitIndex;
                            splitIndex += 200000;
                            WriterTask writerTask = new WriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT());
                            writer.write(writerTask);
                            writerTasks.add(writerTask);
                        }
                        System.out.println(splitIndex - 200000 + " " + index);
                        Message[] pickedMessages = Arrays.copyOfRange(messages, splitIndex - 200000, index);
                        WriterTask writerTask = new WriterTask(pickedMessages, pickedMessages[0].getT(), pickedMessages[pickedMessages.length - 1].getT());
                        writer.write(writerTask);
                        writerTasks.add(writerTask);

                        while (true) {
                            if (check(writerTasks)) {
                                break;
                            }
                        }
                        index = 0;
                        writer.executorService.shutdownNow();
                        Set<Map.Entry<Long, Path>> entries = Reader.lowerMap.entrySet();
                        for (Map.Entry<Long, Path> entriy : entries) {
                            System.out.println(entriy.getValue());
                        }
                    }

                    wait = false;
                }
            }
            while (wait) {

            }
            long start = System.currentTimeMillis();
            List<Message> aa = new ArrayList<>(100000);
            List<ByteBuffer> buffers = Reader.read(tMin, tMax);
            for (ByteBuffer buffer : buffers) {
                buffer.flip();
                while (buffer.position() < buffer.limit()) {

                    long t = buffer.getLong();
                    long a = buffer.getLong();
                    if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                        byte[] b = new byte[34];
                        buffer.get(b, 0, 34);
                        aa.add(new Message(a, t, b));
                    } else {
                        buffer.position(buffer.position() + 34);

                    }
                }
                ((DirectBuffer) buffer).cleaner().clean();
            }
            System.out.println(System.currentTimeMillis()-start);
            return aa;
        }catch (Exception e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public boolean check(List<WriterTask> writerTasks) {
        for (WriterTask task : writerTasks) {
            if (!task.done) {
                return false;
            }
        }
        return true;
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long start = System.currentTimeMillis();
        List<ByteBuffer> buffers = Reader.read(tMin, tMax);
        long total = 0;
        long count = 0;
        for (ByteBuffer buffer : buffers) {
            buffer.flip();
            while (buffer.position() < buffer.limit()) {
                long t = buffer.getLong();
                long a = buffer.getLong();
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    total += a;
                    count += 1;
                    buffer.position(buffer.position() + 34);

                } else {
                    buffer.position(buffer.position() + 34);
                }
            }
            ((DirectBuffer) buffer).cleaner().clean();
        }
        return count == 0 ? 0 : total / count;
    }
}
