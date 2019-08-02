package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
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
        if (!init.get()) {
            if (init.compareAndSet(false, true)) {


                if (index != 0) {
                    System.out.println(1);
                    messages = Arrays.copyOf(messages, index);

                    Arrays.parallelSort(messages, new MessageComparator());
                    System.out.println(1);

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
                    writer.executorService.shutdownNow()
                    ;
                }

                wait = false;
            }
        }
        while (wait) {

        }
        long start = System.currentTimeMillis();
        List<Message> aa = new LinkedList<>();
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
        return aa;
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

//        private NavigableMap<Long, List<Message>> msgMap = new TreeMap<Long, List<Message>>();
//
//    @Override
//    public synchronized void put(Message message) {
//        if (!msgMap.containsKey(message.getT())) {
//            msgMap.put(message.getT(), new ArrayList<Message>());
//        }
//
//        msgMap.get(message.getT()).add(message);
//    }
//
//
//    @Override
//    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        ArrayList<Message> res = new ArrayList<Message>();
//        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
//        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
//            List<Message> msgQueue = mapEntry.getValue();
//            for (Message msg : msgQueue) {
//                if (msg.getA() >= aMin && msg.getA() <= aMax) {
//                    res.add(msg);
//                }
//            }
//        }
//
//        return res;
//    }
//
//
//    @Override
//    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long sum = 0;
//        long count = 0;
//        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
//        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
//            List<Message> msgQueue = mapEntry.getValue();
//            for (Message msg : msgQueue) {
//                if (msg.getA() >= aMin && msg.getA() <= aMax) {
//                    sum += msg.getA();
//                    count++;
//                }
//            }
//        }
//
//        return count == 0 ? 0 : sum / count;
//    }

}
