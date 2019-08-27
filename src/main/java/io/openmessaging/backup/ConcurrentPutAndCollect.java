package io.openmessaging.backup;

import io.openmessaging.Constants;
import io.openmessaging.Message;
import io.openmessaging.backup.ConcurrentMerger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ConcurrentPutAndCollect {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static ThreadLocal<LocalInfo> local = new ThreadLocal<>();
    private static ConcurrentHashMap<Long, LocalInfo> threadMap = new ConcurrentHashMap<>();
    private static CountDownLatch latch = new CountDownLatch(12);
    private static int exitCount = 0;

    private static ByteBuffer buffer0_0 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    private static ByteBuffer buffer0_1 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    private static ByteBuffer buffer0_2 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    private static ByteBuffer buffer0_3 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    private static ByteBuffer buffer0_4 = ByteBuffer.allocate(1000000 * Constants.Message_Size);
    private static ByteBuffer buffer0_5 = ByteBuffer.allocate(1000000 * Constants.Message_Size);

    private static ByteBuffer buffer1_0 = ByteBuffer.allocate(2000000 * Constants.Message_Size);
    private static ByteBuffer buffer1_1 = ByteBuffer.allocate(2000000 * Constants.Message_Size);
    private static ByteBuffer buffer1_2 = ByteBuffer.allocate(2000000 * Constants.Message_Size);

    private static ByteBuffer buffer2_0 = ByteBuffer.allocate(4000000 * Constants.Message_Size);
    private static ByteBuffer buffer2_1 = ByteBuffer.allocate(4000000 * Constants.Message_Size);

    static {
        executorService.execute(new CollectTask());
    }

    private static ByteBuffer mergeAll(List<LocalInfo> list, boolean isEnd) throws Exception {
        Future<ByteBuffer> merge0_0 = ConcurrentMerger.merge(list.get(0).byteBuffer, list.get(1).byteBuffer, buffer0_0, isEnd);
        Future<ByteBuffer> merge0_1 = ConcurrentMerger.merge(list.get(2).byteBuffer, list.get(3).byteBuffer, buffer0_1, isEnd);
        Future<ByteBuffer> merge0_2 = ConcurrentMerger.merge(list.get(4).byteBuffer, list.get(5).byteBuffer, buffer0_2, isEnd);
        Future<ByteBuffer> merge0_3 = ConcurrentMerger.merge(list.get(6).byteBuffer, list.get(7).byteBuffer, buffer0_3, isEnd);
        Future<ByteBuffer> merge0_4 = ConcurrentMerger.merge(list.get(8).byteBuffer, list.get(9).byteBuffer, buffer0_4, isEnd);
        Future<ByteBuffer> merge0_5 = ConcurrentMerger.merge(list.get(10).byteBuffer, list.get(11).byteBuffer, buffer0_5, isEnd);


        Future<ByteBuffer> merge1_0 = ConcurrentMerger.merge(merge0_0.get(), merge0_1.get(), buffer1_0, isEnd);
        Future<ByteBuffer> merge1_1 = ConcurrentMerger.merge(merge0_2.get(), merge0_3.get(), buffer1_1, isEnd);
        Future<ByteBuffer> merge1_2 = ConcurrentMerger.merge(merge0_4.get(), merge0_5.get(), buffer1_2, isEnd);

        Future<ByteBuffer> merge2_0 = ConcurrentMerger.merge(merge1_0.get(), merge1_1.get(), buffer2_0, isEnd);
        Future<ByteBuffer> merge2_1 = ConcurrentMerger.merge(merge2_0.get(), merge1_2.get(), buffer2_1, isEnd);

        return merge2_1.get();
    }

    private static class CollectTask implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(1000);

            } catch (Exception e) {
                e.printStackTrace();
            }

            while (true) {
                try {

                    List<LocalInfo> list = new ArrayList<>(threadMap.values());
                    boolean isFinised = latch.await(2, TimeUnit.SECONDS);
                    if (!isFinised) {
                        exitCount++;
                        if (exitCount == 5) {

                            ByteBuffer buffer;
                            do {
                                buffer = mergeAll(list, true);
                                buffer.flip();
                                while (buffer.hasRemaining()) {
                                    if (!check(buffer.getLong())) {
                                        System.out.println();
                                    }
                                    buffer.position(buffer.position() + Constants.Message_Size - 8);
                                }
                                buffer.clear();

                            } while (buffer.hasRemaining());


                            System.out.println(min);
                            System.exit(-1);
                        }
                    }

                    ByteBuffer buffer = mergeAll(list, false);

//                    buffer.flip();
//                    while (buffer.hasRemaining()) {
//                        if (!check(buffer.getLong())) {
//                            System.out.println();
//                        }
//                        buffer.position(buffer.position() + Constants.Message_Size - 8);
//                    }
                    buffer.clear();

                    if (!isFinised) {
                        int t = 0;
                        System.out.println();
                        for (int i = 0; i < list.size(); i++) {
                            LocalInfo localInfo = list.get(i);
                            t += localInfo.byteBuffer.position() / Constants.Message_Size;

                        }
                        System.out.println(t += buffer0_0.position() / Constants.Message_Size);
                        System.out.println(t += buffer0_1.position() / Constants.Message_Size);
                        System.out.println(t += buffer0_2.position() / Constants.Message_Size);
                        System.out.println(t += buffer0_3.position() / Constants.Message_Size);
                        System.out.println(t += buffer0_4.position() / Constants.Message_Size);
                        System.out.println(t += buffer0_5.position() / Constants.Message_Size);

                        System.out.println(t += buffer1_0.position() / Constants.Message_Size);
                        System.out.println(t += buffer1_1.position() / Constants.Message_Size);
                        System.out.println(t += buffer1_2.position() / Constants.Message_Size);
                        System.out.println(t += buffer2_0.position() / Constants.Message_Size);

                        System.out.println(min);
                    }
                    synchronized (latch) {
                        latch.notifyAll();
                        latch = new CountDownLatch(12);
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
            while (!byteBuffer.hasRemaining()) {
                synchronized (latch) {
                    latch.countDown();
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
}
