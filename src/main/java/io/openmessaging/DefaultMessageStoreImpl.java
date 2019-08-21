package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    private MessageWriter writer = new MessageWriter();
    private MessageReader reader = new MessageReader();

    private AtomicInteger messageCount = new AtomicInteger(0);

    private int batchSize = Constants.Message_Batch_Size;
    private int messageSize = Constants.Message_Size;

    private volatile ByteBuffer messageBuffer = ByteBuffer.allocate(batchSize * messageSize);


    private AtomicInteger concurrentPut = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);

    private volatile boolean readyForRead = false;

    private void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        messageBuffer.putLong(startIndex, message.getT());
        messageBuffer.putLong(startIndex + 8, message.getA());
        for (int i = 0; i < messageSize - 16; i++) {
            messageBuffer.put(startIndex + 16 + i, message.getBody()[i]);
        }
    }

    @Override
    void put(Message message) {
        try {
            concurrentPut.incrementAndGet();
            int count = messageCount.getAndIncrement();
            if (count < batchSize - 1) {
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();

            } else if (count == batchSize - 1) {
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();

                while (concurrentPut.get() != 0) {
                }

                writer.write(new MessageWriterTask(messageBuffer));
                messageBuffer = ByteBuffer.allocate(batchSize * messageSize);
                messageCount.getAndUpdate(x -> 0);
                synchronized (this) {
                    this.notifyAll();
                }
            } else if (count > batchSize - 1) {
                concurrentPut.decrementAndGet();

                synchronized (this) {
                    this.wait();
                }

                concurrentPut.incrementAndGet();
                count = messageCount.getAndIncrement();
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


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
        if (buffer == null) {
            System.out.println("buffer is null");
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
        long sum = 0, count = 0;
        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        reader.fastRead(buffer, tMin, tMax);
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
    }
}
//    private void messageToBuffer(int count, Message message) {
//        messages[count] = message;
//    }


//    private ThreadLocal<Integer> local = new ThreadLocal<>();
//
//
//    private ConcurrentMap<Long, Thread> threadMap = new ConcurrentHashMap<>();
//    private volatile int threadCount = 0;
//
//    private volatile CyclicBarrier barrier;
//
//    private volatile boolean putInited = false;
//
//
//    private void initPut() {
//        threadCount = threadMap.size();
//        barrier = new CyclicBarrier(threadCount, new Runnable() {
//            @Override
//            public void run() {
//                try {
////
////                    Message[] sort = new Message[batchSize * 2];
////                    System.arraycopy(messages1, 0, sort, 0, batchSize);
////                    System.arraycopy(messages, 0, sort, batchSize, batchSize);
////
////                    Arrays.parallelSort(sort, new Comparator<Message>() {
////                        @Override
////                        public int compare(Message o1, Message o2) {
////                            return Long.compare(o1.getT(), o2.getT());
////                        }
////                    });
////
////                    for (int i = 0; i < sort.length; i++) {
////                        if (i != sort[i].getT()) {
////                            System.out.println(i + " " + sort[i].getT());
////                            System.exit(1);
////                        }
////                    }
//                    messages = new Message[batchSize];
//
//                    Thread.sleep(1000);
//                    System.gc();
//                    System.out.println(1);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        putInited = true;
//    }

//    private void putPhase1(Message message) throws InterruptedException {
//        threadMap.putIfAbsent(Thread.currentThread().getId(), Thread.currentThread());
//        concurrentPut.incrementAndGet();
//        int count = messageCount.getAndIncrement();
//        if (count < batchSize - 1) {
//            messageToBuffer(count, message);
//            concurrentPut.decrementAndGet();
//
//        } else if (count == batchSize - 1) {
//            messageToBuffer(count, message);
//            concurrentPut.decrementAndGet();
//
//            while (concurrentPut.get() != 0) ;
//            messages1 = messages;
//            messages = new Message[batchSize];
//            initPut();
//            synchronized (this) {
//                this.notifyAll();
//            }
//        } else if (count > batchSize - 1) {
//            synchronized (this) {
//                concurrentPut.decrementAndGet();
//                this.wait();
//            }
//            put(message);
//        }
//    }

//    private void putPhase2(Message message) throws Exception {
//        Integer index = local.get();
//        if (index == null) {
//            index = (int) Thread.currentThread().getId() % threadCount;
//        }
//        if (index >= messages.length) {
//            barrier.await();
//            index = (int) Thread.currentThread().getId() % threadCount;
//        }
//        messages[index] = message;
//        local.set(index + threadCount);
//    }

//    @Override
//    void put(Message message) {
//        try {
//            if (!putInited) {
//                putPhase1(message);
//            } else {
//                putPhase2(message);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
