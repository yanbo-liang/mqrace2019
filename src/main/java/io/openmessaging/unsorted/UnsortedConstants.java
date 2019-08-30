package io.openmessaging.unsorted;

import io.openmessaging.Constants;

class UnsortedConstants {
    static int Partition_Size = 4000000;
    static int Buffer_Size = (int)(2.5 * Partition_Size * (Constants.Message_Size-8));
    static int Buffer_Queue_Size = (int)(Constants.Direct_Memory_Size / Buffer_Size);
}
