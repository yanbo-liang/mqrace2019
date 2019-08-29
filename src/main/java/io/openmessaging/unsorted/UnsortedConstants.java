package io.openmessaging.unsorted;

import io.openmessaging.Constants;

class UnsortedConstants {
    static int Partition_Size = 2000;
    static int Buffer_Size = 100 * Partition_Size * Constants.Message_Size;
    static long Buffer_Queue_Size = Constants.Direct_Memory_Size / Buffer_Size;
}
