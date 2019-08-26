package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConcurrentMerge {
    private static ExecutorService executorService = Executors.newFixedThreadPool(4);

    private static class MergeTask implements Callable<ByteBuffer> {
        private ByteBuffer firstBuffer, secondBuffer, targetBuffer;

        public MergeTask(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer) {
            this.firstBuffer = firstBuffer;
            this.secondBuffer = secondBuffer;
            this.targetBuffer = targetBuffer;

        }

        @Override
        public ByteBuffer call() throws Exception {
            try {
                firstBuffer.flip();
                secondBuffer.flip();
                int firstIndex = 0, secondIndex = 0, firstLimit = firstBuffer.limit(), secondLimit = secondBuffer.limit();
                long firstT = firstBuffer.getLong(firstIndex);
                long secondT = secondBuffer.getLong(secondIndex);
                while (true) {
                    if (!targetBuffer.hasRemaining()) {
                        break;
                    }
                    if (firstT < secondT) {
                        targetBuffer.put(firstBuffer.array(), firstIndex, Constants.Message_Size);
                        firstIndex += Constants.Message_Size;
                        if (firstIndex >= firstLimit) {
                            break;
                        }
                        firstT = firstBuffer.getLong(firstIndex);
                    } else {
                        targetBuffer.put(secondBuffer.array(), secondIndex, Constants.Message_Size);
                        secondIndex += Constants.Message_Size;
                        if (secondIndex >= secondLimit) {
                            break;
                        }
                        secondT = secondBuffer.getLong(secondIndex);
                    }
                }
                firstBuffer.clear();
                secondBuffer.clear();
                if (firstIndex < firstLimit) {
                    firstBuffer.put(firstBuffer.array(), firstIndex, firstLimit - firstIndex);
                }
                if (secondIndex < secondLimit) {
                    secondBuffer.put(secondBuffer.array(), secondIndex, secondLimit - secondIndex);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return targetBuffer;
        }
    }

    public static Future<ByteBuffer> merge(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer) {
        return executorService.submit(new MergeTask(firstBuffer, secondBuffer, targetBuffer));
    }
}
