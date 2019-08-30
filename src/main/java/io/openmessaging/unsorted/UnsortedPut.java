package io.openmessaging.unsorted;

import io.openmessaging.Constants;
import io.openmessaging.Message;
import io.openmessaging.unsafe.UnsafeBuffer;
import io.openmessaging.unsafe.UnsafeWriter;
import org.omg.CORBA.TCKind;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class UnsortedPut {
    private static UnsafeBuffer unsafeBuffer = UnsortedBufferManager.borrowBuffer();
    private static AtomicInteger count = new AtomicInteger(0);
    private static long min = 0;
    private static long max = 0;
    private static AtomicBoolean init = new AtomicBoolean(false);
    private static CyclicBarrier barrier = new CyclicBarrier(Constants.Thread_Count, () -> {
        if (count.get() * Constants.Message_Size > UnsortedConstants.Buffer_Size) {
            System.out.println("fucker");
            System.exit(1);
        }else{
            System.out.println(count.get());
        }
        min += UnsortedConstants.Partition_Size;
        max += UnsortedConstants.Partition_Size;
        unsafeBuffer.position(count.get() * Constants.Message_Size);
        unsafeBuffer.flip();
        UnsortedWriter.write(unsafeBuffer);
        unsafeBuffer = UnsortedBufferManager.borrowBuffer();
        count.set(0);
    });

    public static void put(Message message) {
        if (!init.get()) {
            synchronized (UnsortedPut.class) {
                if (!init.get()) {
                    init.compareAndSet(false, true);
                    min = message.getT() / UnsortedConstants.Partition_Size * UnsortedConstants.Partition_Size;
                    max = min + UnsortedConstants.Partition_Size - 1;
                }
            }
        }
        while (!(min <= message.getT() && message.getT() <= max)) {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        int index = count.getAndIncrement() * Constants.Message_Size;
        unsafeBuffer.putLong(index, message.getT());
        unsafeBuffer.putLong(index + 8, message.getA());
        unsafeBuffer.put(index + 16, message.getBody());
    }


}
