package io.openmessaging;

public class ByteUtils {
    public static void putLong(byte[] array, long x, int index) {
        array[index + 7] = (byte) (x >> 56);
        array[index + 6] = (byte) (x >> 48);
        array[index + 5] = (byte) (x >> 40);
        array[index + 4] = (byte) (x >> 32);
        array[index + 3] = (byte) (x >> 24);
        array[index + 2] = (byte) (x >> 16);
        array[index + 1] = (byte) (x >> 8);
        array[index + 0] = (byte) (x >> 0);
    }

    public static long getLong(byte[] array, int index) {
        return ((((long) array[index + 7] & 0xff) << 56)
                | (((long) array[index + 6] & 0xff) << 48)
                | (((long) array[index + 5] & 0xff) << 40)
                | (((long) array[index + 4] & 0xff) << 32)
                | (((long) array[index + 3] & 0xff) << 24)
                | (((long) array[index + 2] & 0xff) << 16)
                | (((long) array[index + 1] & 0xff) << 8)
                | (((long) array[index + 0] & 0xff) << 0));
    }


    public static void countSort(byte[] messageBuffer, byte[] result) {
        int size = messageBuffer.length / Constants.Message_Size;
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            long l = getLong(messageBuffer, i * Constants.Message_Size);
            if (l > max) {
                max = l;
            }
            if (l < min) {
                min = l;
            }
        }//这里k的大小是要排序的数组中，元素大小的极值差+1
        int distinct = (int) (max - min + 1);
        int times[] = new int[distinct];
        for (int i = 0; i < size; ++i) {
            long l = getLong(messageBuffer, i * Constants.Message_Size);

            times[(int) (l - min)] += 1;//优化过的地方，减小了数组c的大小
        }
        for (int i = 1; i < times.length; ++i) {
            times[i] = times[i] + times[i - 1];
        }
        for (int i = size - 1; i >= 0; --i) {
            long l = getLong(messageBuffer, i * Constants.Message_Size);
            System.arraycopy(messageBuffer, i * Constants.Message_Size, result, --times[(int) (l - min)] * Constants.Message_Size, Constants.Message_Size);
//            result[--times[(int) (l - min)]] = messageBuffer[i];//按存取的方式取出c的元素
        }
    }

}
