package io.openmessaging.unsafe;

import io.openmessaging.Constants;

public class UnsafeSort {
    public static void countSort(UnsafeBuffer unSortedBuffer, UnsafeBuffer sortedBuffer, int limit) {
        long start = System.currentTimeMillis();
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
        for (int i = 0; i < limit; i += Constants.Message_Size) {
            long l = unSortedBuffer.getLong(i);
            if (l > max) {
                max = l;
            }
            if (l < min) {
                min = l;
            }
        }
        System.out.println("min max " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        //这里k的大小是要排序的数组中，元素大小的极值差+1
        int distinct = (int) (max - min + 1);
        int times[] = new int[distinct];
        System.out.println("new  " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        for (int i = 0; i < limit; i += Constants.Message_Size) {
            long l = unSortedBuffer.getLong(i);
            times[(int) (l - min)] += 1;//优化过的地方，减小了数组c的大小
        }
        System.out.println("times  " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        for (int i = times.length - 2; i >= 0; i--) {
            times[i] = times[i] + times[i + 1];
        }
        System.out.println("add  " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = limit - Constants.Message_Size; i >= 0; i -= Constants.Message_Size) {
            long l = unSortedBuffer.getLong(i);
            UnsafeBuffer.copy(unSortedBuffer.getAddress() + i, sortedBuffer.getAddress() + (--times[(int) (l - min)] * Constants.Message_Size), Constants.Message_Size);
//            System.arraycopy(messageBuffer, i * Constants.Message_Size, sortedBuffer, --times[(int) (l - min)] * Constants.Message_Size, Constants.Message_Size);
//            result[--times[(int) (l - min)]] = messageBuffer[i];//按存取的方式取出c的元素
        }
        System.out.println("copy  " + (System.currentTimeMillis() - start));
    }
}
