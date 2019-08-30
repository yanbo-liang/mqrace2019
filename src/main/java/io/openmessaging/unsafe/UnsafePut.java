//package io.openmessaging.unsafe;
//
//import io.openmessaging.Constants;
//import io.openmessaging.Message;
//import io.openmessaging.MessageWriter;
//;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class UnsafePut {
//    private static UnsafeBuffer unsafeBuffer =  new UnsafeBuffer(Constants.Message_Buffer_Size);
//
//    private static AtomicInteger messageCount = new AtomicInteger(0);
//    private static volatile CountDownLatch latch = new CountDownLatch(Constants.Thread_Count - 1);
//
//    public static void put(Message message) throws Exception {
//        int count = messageCount.getAndIncrement();
//        if (count < Constants.Batch_Size - 1) {
//            putMessage(count, message);
//        } else if (count == Constants.Batch_Size - 1) {
//            putMessage(count, message);
//
//            latch.await(1, TimeUnit.SECONDS);
//            unsafeBuffer.setLimit(Constants.Message_Buffer_Size);
//            MessageWriter.writeToQueue(unsafeBuffer);
//            messageCount.getAndUpdate(x -> 0);
//            synchronized (latch) {
//                latch.notifyAll();
//                latch = new CountDownLatch(11);
//            }
//        } else if (count > Constants.Batch_Size - 1) {
//            synchronized (latch) {
//                latch.countDown();
//                latch.wait();
//            }
//            count = messageCount.getAndIncrement();
//            putMessage(count, message);
//        }
//    }
//
//    public static void putEnd() throws Exception {
//        unsafeBuffer.setLimit(messageCount.get() * Constants.Message_Size);
//        MessageWriter.writeToQueueEnd(unsafeBuffer);
//    }
//
//    private static void putMessage(int count, Message message) {
//        int startIndex = count * Constants.Message_Size;
//        unsafeBuffer.putLong(startIndex, message.getT());
//        unsafeBuffer.putLong(startIndex + 8, message.getA());
//        unsafeBuffer.put(startIndex + 16, message.getBody());
//    }
//}
