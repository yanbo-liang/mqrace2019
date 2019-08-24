package io.openmessaging;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    private volatile long[] messageBuffer = new long[Constants.Message_Buffer_Size];
    private volatile int messageBufferStart;

    private AtomicInteger messageCount = new AtomicInteger(0);

    private int batchSize = Constants.Message_Batch_Size;


    private AtomicInteger concurrentPut = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);

    private volatile boolean readyForRead = false;
    static long s = System.currentTimeMillis();

    static long initStart;

    public DefaultMessageStoreImpl() {
        initStart = System.currentTimeMillis();
    }

    //    private void messageToBuffer(int count, Message message) {
//        int startIndex = count * Constants.Message_Size;
//        messageBuffer.putLong(startIndex, message.getT());
//        messageBuffer.putLong(startIndex + 8, message.getA());
//        for (int i = 0; i < messageSize - 16; i++) {
//            messageBuffer.put(startIndex + 16 + i, message.getBody()[i]);
//        }
//    }
    private void toLong(int count, Message message, long[] data) {
        int startIndex = count * Constants.Message_Long_size;
        data[startIndex] = message.getT();
        data[startIndex + 1] = message.getA();
//        LongArrayUtils.byteArrayToLongArray(data, startIndex + 2, message.getBody());

    }

    //    public boolean enQueue(int value) {
//        if(length == data.length){
//            return false;
//        }
//        data[start++] = value;
//        length++;
//        if(start == data.length){
//            start=0;
//        }
//        return true;
//
//    }
//
//    /** Delete an element from the circular queue. Return true if the operation is successful. */
//    public boolean deQueue() {
//        if(length==0){
//            return false;
//        }
//        data[end++]=0;
//        length--;
//        if(end == data.length){
//            end = 0;
//        }
//        return true;
//    }
//
//    /** Get the front item from the queue. */
//    public int Front() {
//        if(length==0){
//            return -1;
//        }
//        return data[end];
//
//    }
//
//    /** Get the last item from the queue. */
//    public int Rear() {
//        if(length==0){
//            return -1;
//        }
//        int tmp = start-1;
//        if(tmp<0){
//            tmp=data.length-1;
//        }
//        return data[tmp];
//    }
//
//    /** Checks whether the circular queue is empty or not. */
//    public boolean isEmpty() {
//        return length == 0;
//
//    }
//
//    /** Checks whether the circular queue is full or not. */
//    public boolean isFull() {
//        return length==data.length;
//
//    }
    static class ThreadBuffer {
        long[] buffer = new long[1000000 * Constants.Message_Long_size];
        int read = 0;
        int write = 0;

        void add(Message message) {
            buffer[write++] = message.getT();
            buffer[write++] = message.getA();
            buffer[write++] = message.getT();
            if (write == buffer.length) {
                write = 0;
            }
        }
    }

    static ExecutorService executorService1 = Executors.newFixedThreadPool(4);
    static ExecutorService executorService2 = Executors.newFixedThreadPool(2);
    static ExecutorService executorService3 = Executors.newFixedThreadPool(1);

    static ConcurrentHashMap<Long, List<MyArrayBlockingQueue>> map1 = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Long, List<MyArrayBlockingQueue>> map2 = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Long, List<MyArrayBlockingQueue>> map3 = new ConcurrentHashMap<>();

    static {
        MyArrayBlockingQueue queue1_1 = new MyArrayBlockingQueue(10000000);
        MyArrayBlockingQueue queue1_2 = new MyArrayBlockingQueue(10000000);
        MyArrayBlockingQueue queue1_3 = new MyArrayBlockingQueue(10000000);
        MyArrayBlockingQueue queue1_4 = new MyArrayBlockingQueue(10000000);


        MyArrayBlockingQueue queue2_1 = new MyArrayBlockingQueue(15000000);
        MyArrayBlockingQueue queue2_2 = new MyArrayBlockingQueue(15000000);


        map1.put(0L, new ArrayList<>());
        map1.put(1L, new ArrayList<>());
        map1.put(2L, new ArrayList<>());
        map1.put(3L, new ArrayList<>());

        executorService1.execute(new Job(0L, queue1_1, map1, 2000));
        executorService1.execute(new Job(1L, queue1_2, map1, 2000));
        executorService1.execute(new Job(2L, queue1_3, map1, 2000));
        executorService1.execute(new Job(3L, queue1_4, map1, 2000));


        map2.put(0L, new ArrayList<>(Arrays.asList(queue1_1, queue1_2)));
        map2.put(1L, new ArrayList<>(Arrays.asList(queue1_3, queue1_4)));


        executorService2.execute(new Job(0L, queue2_1, map2, 4000));
        executorService2.execute(new Job(1L, queue2_2, map2, 4000));

        map3.put(0L, new ArrayList<>(Arrays.asList(queue2_1, queue2_2)));

        executorService3.execute(new Job(0L, null, map3, 8000));


    }

    static class Job implements Runnable {
        long a;
        MyArrayBlockingQueue queue;
        ConcurrentHashMap<Long, List<MyArrayBlockingQueue>> map;
        long delay;

        public Job(Long a, MyArrayBlockingQueue queue, ConcurrentHashMap<Long, List<MyArrayBlockingQueue>> map, long delay) {
            this.a = a;
            this.queue = queue;
            this.map = map;
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(delay);
                while (true) {
                    List<MyArrayBlockingQueue> blockingQueues = map.get(a);
                    long min = Long.MAX_VALUE;
                    int pointer = -1;
                    for (int i = 0; i < blockingQueues.size(); i++) {
                        MyArrayBlockingQueue queue = blockingQueues.get(i);

                        long take = queue.peek();
                        if (take != -1 && take < min) {
                            pointer = i;
                            min = take;
                        }
                    }
                    if (pointer != -1) {
                        if (queue != null) {
                            queue.put(blockingQueues.get(pointer).poll());
                        } else {
//                            System.out.println(min);
                            check(blockingQueues.get(pointer).poll());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    static long last = -1;
    static long f = 0;

    static void check(long a) {
        f++;
        if (a < last) {
            System.out.println(last + " " + a);
            System.exit(-1);
        } else {
            last = a;

        }
    }

    ThreadLocal<MyArrayBlockingQueue> local = new ThreadLocal<>();

    @Override
    void put(Message message) {
        try {
            MyArrayBlockingQueue threadBuffer = local.get();
            if (threadBuffer == null) {
                threadBuffer = new MyArrayBlockingQueue(5000000);
                local.set(threadBuffer);
                List<MyArrayBlockingQueue> blockingQueues = map1.get((Thread.currentThread().getId() % 4));
                blockingQueues.add(threadBuffer);
            }
            threadBuffer.put(message.getT());

        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    static int c = 0;

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        try {
            while (true) {
                if (c > 200) {
                    break;
                }
                System.out.println(last);
                System.out.println(System.currentTimeMillis() - initStart);
                Thread.sleep(1000);
                c++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.exit(1);
        try {
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!init.get()) {
                if (init.compareAndSet(false, true)) {
                    MessageWriter.write(MessageWriterTask.createEndTask(messageBuffer, messageCount.get()));

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
////                MessageWriter.write(new MessageWriterTask(messageBuffer,Constants.Message_Batch_Size));
////                messageBuffer=new long[Constants.Message_Buffer_Size];
//
//                System.out.println(System.currentTimeMillis()-s);
//                s=System.currentTimeMillis();
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

    private void messageToBuffer(int count, Message message) {
        int startIndex = messageBufferStart + count * Constants.Message_Long_size;
        messageBuffer[startIndex] = message.getT();
        messageBuffer[startIndex + 1] = message.getA();
//        messageBuffer[startIndex+2]=ByteBuffer.wrap(message.getBody()).getLong();
//        LongArrayUtils.byteArrayToLongArray(messageBuffer, startIndex + 2, message.getBody());
//
//        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
//        LongArrayUtils.longArraytoByteBuffer(messageBuffer, startIndex + 2, byteBuffer);
//        if (Arrays.equals(message.getBody(),byteBuffer.array())){
//        }else {
//            System.out.println(message.getT());
//            System.out.println(Arrays.toString(message.getBody()));
//            System.out.println(Arrays.toString(byteBuffer.array()));
//            System.out.println(messageBuffer[startIndex+2]);
//
//
//                System.exit(-1);
//        }
    }

}