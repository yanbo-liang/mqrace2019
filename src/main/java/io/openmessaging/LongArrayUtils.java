//package io.openmessaging;
//
//import java.nio.ByteBuffer;
//
//public class LongArrayUtils {
//
//    public static void byteArrayToLongArray(long[] messageBuffer, int start, byte[] data) {
//        for (int i = 0; i < data.length; i++) {
//            int bufferIndex = i / 8 + start;
//            int byteIndex = i % 8;
//            int shiftIndex = 56 - 8 * (byteIndex );
//            long a = (data[i] & 0xffL) << shiftIndex;
//            messageBuffer[bufferIndex] = a ^ messageBuffer[bufferIndex];
//        }
//    }
//
//    public static void longArraytoByteBuffer(long[] messageBuffer, int start, ByteBuffer buffer) {
//        for (int i = 0; i < Constants.Message_Size - 16; i++) {
//            int bufferIndex = i / 8 + start;
//            int byteIndex = i % 8;
//            int shiftIndex = 64 - 8 * (byteIndex + 1);
//            byte a = (byte) (messageBuffer[bufferIndex] >> shiftIndex);
//            buffer.put(a);
//        }
//    }
//
//    public static void countSort(long[] messageBuffer, long[] sortMessageBuffer, int messageBufferCount) {
//        long start = System.currentTimeMillis();
//        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
//        for (int i = 0; i < messageBufferCount; i++) {
////            long l = messageBuffer[i * Constants.Message_Long_size];
////            if (l > max) {
////                max = l;
////            }
////            if (l < min) {
////                min = l;
////            }
////        }
//        long start1 = System.currentTimeMillis();
//        System.out.println("min max " + (start1 - start));
//        //这里k的大小是要排序的数组中，元素大小的极值差+1
//        int distinct = (int) (max - min + 1);
//        int times[] = new int[distinct];
//        long start2 = System.currentTimeMillis();
//
//        System.out.println("new  " + (start2 - start1));
//
//        for (int i = 0; i < messageBufferCount; i++) {
////            long l = messageBuffer[i * Constants.Message_Long_size];
//            times[(int) (l - min)] += 1;//优化过的地方，减小了数组c的大小
//        }
//
//        for (int i = times.length - 2; i >= 0; i--) {
//            times[i] = times[i] + times[i + 1];
//        }
//
//        long start3 = System.currentTimeMillis();
//
//        System.out.println("times  " + (start3 - start2));
//
//        for (int i = messageBufferCount - 1; i >= 0; i--) {
//            long l = messageBuffer[i * Constants.Message_Long_size];
//            System.arraycopy(messageBuffer, i * Constants.Message_Long_size, sortMessageBuffer, --times[(int) (l - min)] * Constants.Message_Long_size, Constants.Message_Long_size);
////            result[--times[(int) (l - min)]] = messageBuffer[i];//按存取的方式取出c的元素
//        }
//
//        long start4 = System.currentTimeMillis();
//
//        System.out.println("copy  " + (start4 - start3));
//    }
//}
