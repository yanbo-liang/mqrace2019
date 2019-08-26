package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConcurrentMerger {
    private static ExecutorService executorService = Executors.newFixedThreadPool(8);

    public static Future<ByteBuffer> merge(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer, boolean isEnd) {
        return executorService.submit(new MergeTask(firstBuffer, secondBuffer, targetBuffer, isEnd));
    }

    private static class MergeTask implements Callable<ByteBuffer> {
        private ByteBuffer firstBuffer, secondBuffer, targetBuffer;
        private boolean isEnd;

        public MergeTask(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer, boolean isEnd) {
            this.firstBuffer = firstBuffer;
            this.secondBuffer = secondBuffer;
            this.targetBuffer = targetBuffer;
            this.isEnd = isEnd;
        }

        @Override
        public ByteBuffer call() throws Exception {
            try {
                if (!isEnd) {
                    if (firstBuffer.position() == 0) {
                        return targetBuffer;
                    }
                    if (secondBuffer.position() == 0) {
                        return targetBuffer;
                    }
                }
                firstBuffer.flip();
                secondBuffer.flip();

                int firstIndex = 0, secondIndex = 0, firstLimit = firstBuffer.limit(), secondLimit = secondBuffer.limit();
                long firstT;
                long secondT;
                if (!isEnd) {
                    firstT = firstBuffer.getLong(firstIndex);
                    secondT = secondBuffer.getLong(secondIndex);
                } else {
                    if (firstLimit == 0 && secondLimit == 0) {
                        return targetBuffer;
                    }
                    firstT = firstLimit != 0 ? firstBuffer.getLong(firstIndex) : Long.MAX_VALUE;
                    secondT = secondLimit != 0 ? secondBuffer.getLong(secondIndex) : Long.MAX_VALUE;
                }

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
}
