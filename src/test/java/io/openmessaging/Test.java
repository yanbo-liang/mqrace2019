package io.openmessaging;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Test {
    public static void main(String[] args) {


        Message m3 = new Message(3, 3, null);
        Message m2 = new Message(2, 2, null);
        Message m1 = new Message(1, 1, null);
        Message[] messages = new Message[3];
        messages[0] = m3;
        messages[1] = m2;
        messages[2] = m1;
        Arrays.parallelSort(messages, new MessageComparator());
        for (int i = 0;i<3;i++){
            System.out.println(messages[i].getT());
        }

        Message[] messages1 = Arrays.copyOfRange(messages, 1,5);
        System.out.println(messages1.length);
//        for (int i = 0;i<messages1.length;i++){
//            System.out.println(messages1[i].getT());
//        }
//        List<ByteBuffer> a = new ArrayList<>();
//        while(true) {
//            try {
//                Path path = Paths.get("/Users/yanbo.liang/test/0-200006");
//
//                AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
//                ByteBuffer buffer = ByteBuffer.allocateDirect(100 * 1024 * 1024);
//                fileChannel.read(buffer, 0, null, new CompletionHandler<Integer, ByteBuffer>() {
//
//                    @Override
//                    public void completed(Integer result, ByteBuffer attachment) {
//                        System.out.println("bytes written: " + result);
//                        a.add(buffer);
//                        ((DirectBuffer)buffer).cleaner().clean();
//                    }
//
//                    @Override
//                    public void failed(Throwable e, ByteBuffer attachment) {
//                        System.out.println("Write failed");
//                        e.printStackTrace();
//                    }
//                });
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }


//        long start = System.currentTimeMillis();
//        List<Message> list = new LinkedList<>();
//        for (int i = 0; i < 10000000; i++) {
//            byte[] data = new byte[34];
//            list.add(new Message(1, 1, data));
//        }
//        System.out.println(System.currentTimeMillis() - start);
//        start = System.currentTimeMillis();
//        Collections.sort(list, new Comparator<Message>() {
//            @Override
//            public int compare(Message o1, Message o2) {
//                return 0;
//            }
//        });
//        System.out.println(System.currentTimeMillis() - start);
//
//        try {
//            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
//            theUnsafe.setAccessible(true);
//            Unsafe unsafe = (Unsafe) theUnsafe.get(null);
//            unsafe.allocateMemory(1);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
