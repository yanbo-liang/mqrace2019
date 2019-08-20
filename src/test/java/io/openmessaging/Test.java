package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Test {

    private static ExecutorService executor = Executors.newFixedThreadPool(4);
    private static CountDownLatch latch = new CountDownLatch(4);

    private static class TaskResult {
        long min;
        long max;

        public TaskResult(long min, long max) {
            this.min = min;
            this.max = max;
        }
    }

    private static class TestTask implements Callable<TaskResult> {
        private ByteBuffer buffer;
        private int start;

        public TestTask(ByteBuffer buffer, int start) {
            this.buffer = buffer;
            this.start = start;
        }

        @Override
        public TaskResult call() throws Exception {
            long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
            for (int i = start; i < 125 * 1024 * 1024 + start; i += Constants.Message_Size) {
                long l = buffer.getLong(i);
                if (l > max) {
                    max = l;
                }
                if (l < min) {
                    min = l;
                }
            }
            latch.countDown();
            return new TaskResult(min, max);

        }

    }

    public static void main(String[] args) {
        try {


            ByteBuffer buffer = ByteBuffer.allocate(500 * 1024 * 1024);
            long a = 0;
            while (buffer.hasRemaining()) {
                buffer.putLong(a++);
            }
            buffer.flip();

            long start = System.currentTimeMillis();

            List<Future<TaskResult>> futureList = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Future<TaskResult> submit = executor.submit(new TestTask(buffer, i * 125 * 1024 * 1024));
                futureList.add(submit);

            }
            latch.await();
            for (int i = 0; i < futureList.size(); i++) {
                Future<TaskResult> taskResultFuture = futureList.get(i);

                System.out.println(taskResultFuture.get().min);
                System.out.println(taskResultFuture.get().max);

            }


            executor.shutdown();

//            long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
//            for (int i = 0; i < 500 * 1024 * 1024; i += Constants.Message_Size) {
//                long l = buffer.getLong(i);
//                if (l > max) {
//                    max = l;
//                }
//                if (l < min) {
//                    min = l;
//                }
//            }
//            System.out.println(min+" "+max);
            System.out.println(System.currentTimeMillis() - start);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
