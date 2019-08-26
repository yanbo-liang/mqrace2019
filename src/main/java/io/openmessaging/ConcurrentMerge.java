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
                if (firstBuffer.position() == 0) {
                    return targetBuffer;
                }
                if (secondBuffer.position() == 0) {
                    return targetBuffer;
                }
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

    private static class MergeEndTask implements Callable<ByteBuffer> {
        private ByteBuffer firstBuffer, secondBuffer, targetBuffer;

        public MergeEndTask(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer) {
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
                if (firstLimit == 0 && secondLimit == 0) {
                    return targetBuffer;
                }
                long firstT = firstLimit != 0 ? firstBuffer.getLong(firstIndex) : Long.MAX_VALUE;
                long secondT = secondLimit != 0 ? secondBuffer.getLong(secondIndex) : Long.MAX_VALUE;
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

//                firstBuffer.flip();
//                secondBuffer.flip();

//                int firstIndex = 0, secondIndex = 0, firstLimit = firstBuffer.limit(), secondLimit = secondBuffer.limit();
//                int remaining = targetBuffer.remaining();
//                if (remaining > 0) {
//                    if (firstIndex < firstLimit) {
//                        remaining = Math.min(remaining, firstLimit - firstIndex);
//                        targetBuffer.put(firstBuffer.array(), firstIndex, remaining);
//                        firstIndex += remaining;
//                        firstBuffer.clear();
//                        firstBuffer.put(firstBuffer.array(), firstIndex, firstLimit - firstIndex);
//
//                    }
//                    if (secondIndex < secondLimit) {
//                        remaining = Math.min(remaining, secondLimit - secondIndex);
//                        targetBuffer.put(secondBuffer.array(), secondIndex, remaining);
//                        secondIndex += remaining;
//                        secondBuffer.clear();
//                        secondBuffer.put(secondBuffer.array(), secondIndex, secondLimit - secondIndex);
//                    }
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return targetBuffer;
        }
    }

    public static Future<ByteBuffer> merge(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer) {
        return executorService.submit(new MergeTask(firstBuffer, secondBuffer, targetBuffer));
    }

    public static Future<ByteBuffer> mergeEnd(ByteBuffer firstBuffer, ByteBuffer secondBuffer, ByteBuffer targetBuffer) {
        return executorService.submit(new MergeEndTask(firstBuffer, secondBuffer, targetBuffer));
    }

}
