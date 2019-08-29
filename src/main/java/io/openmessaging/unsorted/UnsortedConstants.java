package io.openmessaging.unsorted;

import io.openmessaging.Constants;

class UnsortedConstants {
    static int Partition_Size = 20000000;
    static int Buffer_Size = 3 * Partition_Size * Constants.Message_Size;
    static int Buffer_Queue_Size = (int)(Constants.Direct_Memory_Size / Buffer_Size);
}
