package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ConcurrentPutAndCollect {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static ThreadLocal<LocalInfo> local = new ThreadLocal<>();
    private static ConcurrentHashMap<Long, LocalInfo> threadMap = new ConcurrentHashMap<>();
    private static CountDownLatch latch = new CountDownLatch(8);
    private static int exitCount = 0;

    static {
        executorService.execute(new CollectTask());
    }

    static ByteBuffer buffer1_0 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    static ByteBuffer buffer1_1 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    static ByteBuffer buffer1_2 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    static ByteBuffer buffer1_3 = ByteBuffer.allocate(1000000 * Constants.Message_Size);

    static ByteBuffer buffer2_1 = ByteBuffer.allocate(2000000 * Constants.Message_Size);
    static ByteBuffer buffer2_2 = ByteBuffer.allocate(2000000 * Constants.Message_Size);

    static ByteBuffer buffe3_1 = ByteBuffer.allocate(4000000 * Constants.Message_Size);

    private static void end(List<LocalInfo> list) {
        Future<ByteBuffer> merge1 = ConcurrentMerge.mergeEnd(list.get(0).byteBuffer, list.get(1).byteBuffer, buffer1_0);
        Future<ByteBuffer> merge2 = ConcurrentMerge.mergeEnd(list.get(2).byteBuffer, list.get(3).byteBuffer, buffer1_1);
        Future<ByteBuffer> merge3 = ConcurrentMerge.mergeEnd(list.get(4).byteBuffer, list.get(5).byteBuffer, buffer1_2);
        Future<ByteBuffer> merge4 = ConcurrentMerge.mergeEnd(list.get(6).byteBuffer, list.get(7).byteBuffer, buffer1_3);
        while (!(merge1.isDone() && merge2.isDone() && merge3.isDone() && merge4.isDone())) {

        }


        Future<ByteBuffer> merge5 = ConcurrentMerge.mergeEnd(buffer1_0, buffer1_1, buffer2_1);
        Future<ByteBuffer> merge6 = ConcurrentMerge.mergeEnd(buffer1_2, buffer1_3, buffer2_2);
        while (!(merge5.isDone() && merge6.isDone())) {

        }

        Future<ByteBuffer> merge7 = ConcurrentMerge.mergeEnd(buffer2_1, buffer2_2, buffe3_1);
        while (!(merge7.isDone())) {
        }
    }

    private static class CollectTask implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(2000);

            } catch (Exception e) {
                e.printStackTrace();
            }

            while (true) {
                try {

                    List<LocalInfo> list = new ArrayList<>(threadMap.values());
                    boolean isFinised = latch.await(2, TimeUnit.SECONDS);
                    if (!isFinised) {
                        exitCount++;
                        if (exitCount == 3) {

                            end(list);
                            buffe3_1.flip();
                            while (buffe3_1.hasRemaining()) {
                                if (!check(buffe3_1.getLong())) {
                                    System.out.println();
                                }
                                buffe3_1.position(buffe3_1.position() + Constants.Message_Size - 8);
                            }
                            buffe3_1.clear();
                            end(list);
                            buffe3_1.flip();
                            while (buffe3_1.hasRemaining()) {
                                if (!check(buffe3_1.getLong())) {
                                    System.out.println();
                                }
                                buffe3_1.position(buffe3_1.position() + Constants.Message_Size - 8);
                            }
                            buffe3_1.clear();
                            end(list);
                            buffe3_1.flip();
                            while (buffe3_1.hasRemaining()) {
                                if (!check(buffe3_1.getLong())) {
                                    System.out.println();
                                }
                                buffe3_1.position(buffe3_1.position() + Constants.Message_Size - 8);
                            }
                            buffe3_1.clear();
                            System.out.println(min);
                            System.exit(-1);
                        }
                    }
                    Future<ByteBuffer> merge1 = ConcurrentMerge.merge(list.get(0).byteBuffer, list.get(1).byteBuffer, buffer1_0);
                    Future<ByteBuffer> merge2 = ConcurrentMerge.merge(list.get(2).byteBuffer, list.get(3).byteBuffer, buffer1_1);
                    Future<ByteBuffer> merge3 = ConcurrentMerge.merge(list.get(4).byteBuffer, list.get(5).byteBuffer, buffer1_2);
                    Future<ByteBuffer> merge4 = ConcurrentMerge.merge(list.get(6).byteBuffer, list.get(7).byteBuffer, buffer1_3);
                    while (!(merge1.isDone() && merge2.isDone() && merge3.isDone() && merge4.isDone())) {

                    }


                    Future<ByteBuffer> merge5 = ConcurrentMerge.merge(buffer1_0, buffer1_1, buffer2_1);
                    Future<ByteBuffer> merge6 = ConcurrentMerge.merge(buffer1_2, buffer1_3, buffer2_2);
                    while (!(merge5.isDone() && merge6.isDone())) {

                    }

                    Future<ByteBuffer> merge7 = ConcurrentMerge.merge(buffer2_1, buffer2_2, buffe3_1);
                    while (!(merge7.isDone())) {
                    }
                    buffe3_1.flip();
                    while (buffe3_1.hasRemaining()) {
                        if (!check(buffe3_1.getLong())) {
                            System.out.println();
                        }
                        buffe3_1.position(buffe3_1.position() + Constants.Message_Size - 8);
                    }
                    buffe3_1.clear();

                    if (!isFinised) {
                        int t = 0;
                        System.out.println();
                        for (int i = 0; i < list.size(); i++) {
                            LocalInfo localInfo = list.get(i);
                            t += localInfo.byteBuffer.position() / Constants.Message_Size;

                        }
                        System.out.println(t += buffer1_0.position() / Constants.Message_Size);
                        System.out.println(t += buffer1_1.position() / Constants.Message_Size);
                        System.out.println(t += buffer1_2.position() / Constants.Message_Size);
                        System.out.println(t += buffer1_3.position() / Constants.Message_Size);
                        System.out.println(t += buffer2_1.position() / Constants.Message_Size);
                        System.out.println(t += buffer2_2.position() / Constants.Message_Size);
                        System.out.println(min);
                    }
                    synchronized (latch) {
                        latch.notifyAll();
                        latch = new CountDownLatch(8);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static long min = -1;

    private static boolean check(long a) {
        if (a == min + 1) {
            min = a;
            return true;
        } else {
            System.out.println(a);
            return false;
        }
    }

    private static class LocalInfo {
        ByteBuffer byteBuffer = ByteBuffer.allocate(500000 * Constants.Message_Size);
    }

    static void put(Message message) {
        try {
            LocalInfo localInfo = local.get();
            if (localInfo == null) {
                localInfo = new LocalInfo();
                threadMap.put(Thread.currentThread().getId(), localInfo);
                local.set(localInfo);
            }
            ByteBuffer byteBuffer = localInfo.byteBuffer;
            if (!byteBuffer.hasRemaining()) {
                synchronized (latch) {
                    latch.countDown();
                    System.out.println(latch.getCount());
                    latch.wait();
                }
            }
            byteBuffer.putLong(message.getT());
            byteBuffer.putLong(message.getA());
            byteBuffer.put(message.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void calculate(int threadCount) {

    }
}
