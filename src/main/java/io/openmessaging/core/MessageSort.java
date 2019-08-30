package io.openmessaging.core;

import io.openmessaging.Constants;

class MessageSort {

    static void countSort(MessageBatchWrapper unsorted, MessageBatchWrapper sorted, int size) {
        long[] tUnsorted = unsorted.tArray;
        long[] tSorted = sorted.tArray;
        long[] aUnsorted = unsorted.aArray;
        long[] aSorted = sorted.aArray;
        byte[] bodyUnsorted = unsorted.bodyArray;
        byte[] bodySorted = sorted.bodyArray;
        long start = System.currentTimeMillis();
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
        for (int i = 0; i < size; i += 1) {
            long l = tUnsorted[i];
            if (l > max) {
                max = l;
            }
            if (l < min) {
                min = l;
            }
        }
        System.out.println("min max " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        int distinct = (int) (max - min + 1);
        int[] times = new int[distinct];
        System.out.println("new  " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        for (int i = 0; i < size; i += 1) {
            long l = tUnsorted[i];
            times[(int) (l - min)] += 1;
        }
        System.out.println("times  " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
//        for (int i = times.length - 2; i >= 0; i--) {
//            times[i] = times[i] + times[i + 1];
//        }
        for (int i = 1; i < times.length; i++) {
            times[i] = times[i] + times[i - 1];
        }
        System.out.println("add  " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = size - 1; i >= 0; i -= 1) {
            long l = tUnsorted[i];
            int index = --times[(int) (l - min)];
            tSorted[index] = l;
            aSorted[index] = aUnsorted[i];
            System.arraycopy(bodyUnsorted, i * Constants.Body_Size, bodySorted, index * Constants.Body_Size, Constants.Body_Size);

//            UnsafeBuffer.copy(unsorted.getAddress() + i, sorted.getAddress() + (--times[(int) (l - min)] * Constants.Message_Size), Constants.Message_Size);
//            System.arraycopy(messageBuffer, i * Constants.Message_Size, sorted, --times[(int) (l - min)] * Constants.Message_Size, Constants.Message_Size);
//            result[--times[(int) (l - min)]] = messageBuffer[i];//按存取的方式取出c的元素
        }
        System.out.println("copy  " + (System.currentTimeMillis() - start));
    }
}
