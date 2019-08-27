package io.openmessaging.backup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Dump {

//
//    Message[] messages = new Message[batchSize];
//
//    private void messageToBuffer(int count, Message message) {
//        messages[count] = message;
//    }
//
//
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
//    private long start = System.currentTimeMillis();
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
////
////                    try {
////                    } catch (Exception e) {
////                        e.printStackTrace();
////                    }
////                    Arrays.parallelSort(messages, new Comparator<Message>() {
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
//                    long end = System.currentTimeMillis();
//                    System.out.println(end - start);
//                    start = end;
//                    System.out.println(1);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        putInited = true;
//    }
//
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
//
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
//
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
//ThreadLocal<DefaultMessageStoreImpl.ThreadBuffer> local = new ThreadLocal<>();
//    AtomicBoolean threadCountInit = new AtomicBoolean(false);
//    CyclicBarrier barrier;
//    AtomicInteger waitThread = new AtomicInteger(0);
//
//    ConcurrentHashMap<Long, DefaultMessageStoreImpl.ThreadBuffer> map1 = new ConcurrentHashMap<>();
//    int a = 0;
//
//    @Override
//    void put(Message message) {
//        try {
//            DefaultMessageStoreImpl.ThreadBuffer localInfo = local.get();
//            if (localInfo == null) {
//                localInfo = new DefaultMessageStoreImpl.ThreadBuffer();
//                local.set(localInfo);
//                map1.put(Thread.currentThread().getId(), localInfo);
//            }
//            toLong(localInfo.i, message, localInfo.data);
//            int i = ++localInfo.i;
//            if (i == 500000) {
//                if (!threadCountInit.get()) {
//                    if (threadCountInit.compareAndSet(false, true)) {
//                        while (waitThread.get() == (map1.size() - 1)) {
//
//                        }
//                        barrier = new CyclicBarrier(map1.size(), new Runnable() {
//                            @Override
//                            public void run() {
//                                map1.forEach((x, y) -> {
//                                    y.i = 0;
//                                });
//                                a++;
//                                if (a > 100) {
//                                    System.exit(-1);
//                                }
//                                System.out.println(System.currentTimeMillis() - s);
//                                s = System.currentTimeMillis();
//                            }
//                        });
//                        synchronized (this) {
//                            this.notifyAll();
//                        }
//                    }
//                }
//                if (barrier == null) {
//                    synchronized (this) {
//                        waitThread.incrementAndGet();
//                        this.wait();
//                    }
//                }
//                barrier.await();
//            }
//
//
//        } catch (
//                Exception e) {
//            e.printStackTrace();
//        }
//
//    }

}
