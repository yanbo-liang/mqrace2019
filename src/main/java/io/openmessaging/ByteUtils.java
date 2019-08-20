package io.openmessaging;

import java.nio.ByteBuffer;

public class ByteUtils {

    public static void countSort(ByteBuffer messageBuffer, byte[] result) {
        long start = System.currentTimeMillis();
        int size = messageBuffer.limit() / Constants.Message_Size;
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            long l =messageBuffer.getLong(i * Constants.Message_Size);
            if (l > max) {
                max = l;
            }
            if (l < min) {
                min = l;
            }
        }
        long start1 = System.currentTimeMillis();
        System.out.println("min max "+ (start1-start));
        //这里k的大小是要排序的数组中，元素大小的极值差+1
        int distinct = (int) (max - min + 1);
        int times[] = new int[distinct];
        long start2 = System.currentTimeMillis();

        System.out.println("new  "+ (start2-start1));

        for (int i = 0; i < size; ++i) {
            long l =messageBuffer.getLong(i * Constants.Message_Size);

            times[(int) (l - min)] += 1;//优化过的地方，减小了数组c的大小
        }

        for (int i = 1; i < times.length; ++i) {
            times[i] = times[i] + times[i - 1];
        }

        long start3 = System.currentTimeMillis();

        System.out.println("times  "+ (start3-start2));

        for (int i = size - 1; i >= 0; --i) {
            long l =messageBuffer.getLong(i * Constants.Message_Size);
            System.arraycopy(messageBuffer.array(), i * Constants.Message_Size, result, --times[(int) (l - min)] * Constants.Message_Size, Constants.Message_Size);
//            result[--times[(int) (l - min)]] = messageBuffer[i];//按存取的方式取出c的元素
        }

        long start4 = System.currentTimeMillis();

        System.out.println("copy  "+ (start4-start3));
    }
}
