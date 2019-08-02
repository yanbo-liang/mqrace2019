package io.openmessaging;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    @Override
    synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
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
        return 0;
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
