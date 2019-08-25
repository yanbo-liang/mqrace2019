package io.openmessaging;


import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    //    private volatile long[] messageBuffer = new long[Constants.Message_Buffer_Size];
    private volatile ByteBuffer messageBuffer = ByteBuffer.allocate(Constants.Message_Size * Constants.Message_Batch_Size);

    private volatile int messageBufferStart;

    private AtomicInteger messageCount = new AtomicInteger(0);

    private int batchSize = Constants.Message_Batch_Size;


    private AtomicInteger concurrentPut = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);

    private volatile boolean readyForRead = false;
    static long s = System.currentTimeMillis();

    static long initStart;

//    public DefaultMessageStoreImpl() {
//        executorService.execute(new Task());
//        initStart = System.currentTimeMillis();
//    }
//
    private void messageToBuffer(int count, Message message) {
//        int startIndex = count * Constants.Message_Size;
//        messageBuffer.putLong(startIndex, message.getT());
//        messageBuffer.putLong(startIndex + 8, message.getA());
//        for (int i = 0; i < Constants.Message_Size - 16; i++) {
//            messageBuffer.put(startIndex + 16 + i, message.getBody()[i]);
//        }
    }

    //    private class LocalInfo {
//        int i = 0;
//    }
//    private int lo = 1000000;
//    private int tc = 10;
//    private CountDownLatch latch = new CountDownLatch(tc);
//    private ByteBuffer testBuffer = ByteBuffer.allocate(lo * Constants.Message_Size * tc);
//    private ThreadLocal<Integer> local = new ThreadLocal<>();
//    private ExecutorService executorService = Executors.newSingleThreadExecutor();
//    int a = 0;
//
//    private class Task implements Runnable {
//        @Override
//        public void run() {
//            try {
//                while (true) {
//                    latch.await(2, TimeUnit.SECONDS);
//                    synchronized (DefaultMessageStoreImpl.class) {
//
//
//                        latch = new CountDownLatch(tc);
//                        System.out.println(++a);
//                        System.out.println(System.currentTimeMillis() - s);
//                        s = System.currentTimeMillis();
//                        DefaultMessageStoreImpl.class.notifyAll();
//                    }
//
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
//    }
//
//
//    @Override
//    void put(Message message) {
//        try {
//            Integer localInfo = local.get();
//            if (localInfo == null) {
//                localInfo = 0;
//            }
//            long tid= Thread.currentThread().getId();
//
//            int partStart = ((int) tid % tc) * lo + localInfo;
//
//            int startIndex = partStart * Constants.Message_Size;
//            testBuffer.putLong(startIndex, message.getT());
//            testBuffer.putLong(startIndex + 8, message.getA());
//            for (int i = 0; i < Constants.Message_Size - 16; i++) {
//                testBuffer.put(startIndex + 16 + i, message.getBody()[i]);
//            }
//            localInfo += 1;
//            if (localInfo == lo) {
//                localInfo = 0;
//                synchronized (DefaultMessageStoreImpl.class) {
//                    latch.countDown();
//                    DefaultMessageStoreImpl.class.wait();
//                }
//
//            }
//            local.set(localInfo);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//    @Override
//    void put(Message message) {
//        try {
//            concurrentPut.incrementAndGet();
//            int count = messageCount.getAndIncrement();
//            if (count < batchSize - 1) {
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//
//            } else if (count == batchSize - 1) {
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//
//                while (concurrentPut.get() != 0) {
//                }
//
//                messageBuffer = ByteBuffer.allocate(Constants.Message_Size * Constants.Message_Batch_Size);
//                System.out.println(System.currentTimeMillis() - s);
//                s = System.currentTimeMillis();
//                messageCount.getAndUpdate(x -> 0);
//                synchronized (this) {
//                    this.notifyAll();
//                }
//            } else if (count > batchSize - 1) {
//                synchronized (this) {
//                    concurrentPut.decrementAndGet();
//                    this.wait();
//                }
//
//                concurrentPut.incrementAndGet();
//                count = messageCount.getAndIncrement();
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.out.println(System.currentTimeMillis() - initStart);
        System.exit(1);
        try {
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!init.get()) {
                if (init.compareAndSet(false, true)) {
//                    MessageWriter.write(MessageWriterTask.createEndTask(messageBuffer, messageCount.get()));

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
            List<Message> messageList = new ArrayList<>();
            while (buffer.hasRemaining()) {
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

//                MessageWriter.write(new MessageWriterTask(messageBuffer,Constants.Message_Batch_Size));
//                messageBuffer=new long[Constants.Message_Buffer_Size];

                System.out.println(System.currentTimeMillis()-s);
                s=System.currentTimeMillis();
                messageCount.getAndUpdate(x -> 0);
                synchronized (this) {
                    this.notifyAll();
                }
            } else if (count > batchSize - 1) {
                synchronized (this) {
                    concurrentPut.decrementAndGet();
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

//    private void messageToBuffer(int count, Message message) {
//        int startIndex = messageBufferStart + count * Constants.Message_Long_size;
//        messageBuffer[startIndex] = message.getT();
//        messageBuffer[startIndex + 1] = message.getA();
////        LongArrayUtils.byteArrayToLongArray(messageBuffer, startIndex + 2, message.getBody());
//    }

//private void toLong(int count, Message message, long[] data) {
//    int startIndex = count * Constants.Message_Long_size;
//    data[startIndex] = message.getT();
//    data[startIndex + 1] = message.getA();
////        LongArrayUtils.byteArrayToLongArray(data, startIndex + 2, message.getBody());
//}

}