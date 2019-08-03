package io.openmessaging;


import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

public class Test {
    public static void main(String[] args) {

        try {
            while (true) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(15 * 1024 * 1024);

                Path path = Paths.get("/Users/yanbo.liang/1.mkv");
                AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
                Future<Integer> readFuture = fileChannel.read(buffer, 0);
                fileChannel.close();
            }

//            while (!readFuture.isDone());

        } catch (Exception e) {
            e.printStackTrace();
        }

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
