package io.openmessaging;


import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

public class IOTest {
    static int totalSize = 50 * 10000000;
    static int partSize = 2000000;

    public static void write(Path path) {
        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
            fileChannel.write(byteBuffer);
            fileChannel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long read1(Path path) {
        try {


            ByteBuffer buffer = ByteBuffer.allocateDirect(partSize);
            long start = System.currentTimeMillis();

            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            buffer.limit(partSize);
            int random = ThreadLocalRandom.current().nextInt(totalSize - partSize);
            fileChannel.read(buffer, random);
            fileChannel.close();
            buffer.flip();

            while (buffer.position() < buffer.limit()) {
                if (buffer.get() == -1) {
                    System.out.println();
                }else{
                }
            }
            long end = System.currentTimeMillis();

            return end - start;
        } catch (Exception e) {
            e.printStackTrace();

        }
        return 0;
    }

    public static long read2(Path path) {
        try {
            long start = System.currentTimeMillis();

            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            int random = ThreadLocalRandom.current().nextInt(totalSize - partSize);


            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, random, partSize);

            while (buffer.position() < buffer.limit()) {
                if (buffer.get() == -1) {
                    System.out.println();
                }
            }
            long end = System.currentTimeMillis();

            return end - start;

        } catch (Exception e) {
            e.printStackTrace();

        }
        return 0;
    }

    public static void main(String[] args) {

        long read1Time = 0;
        long read2Time = 0;
        for (int i = 0; i < 1; i++) {
            Path path = Paths.get("/Users/yanbo.liang/test/a" + i);
            write(path);

            read1Time += read1(path);
            read2Time += read2(path);
        }
        System.out.println(read1Time/(double)1);
        System.out.println(read2Time/(double)1);

//        long start = System.currentTimeMillis();
//
//        byte[] a = new byte[30 * 1024 * 1024];
//        for (int i = 1; i < a.length; i++) {
//
//            if (a[i] == 0) {
//            }
//
//        }
////        List<A> a = new ArrayList<>();
////        for (int i = 0; i < 1000000; i++) {
////            a.add(new A());
////        }
//        System.out.println(System.currentTimeMillis() - start);


//
//
//                long c = 0;
//                while (map.position() < map.limit()) {
//                    map.get();
//                    c++;
//                }
//                map.isLoaded()
//                fileChannel.read(buffer, 0);

//                fileChannel.close();
//                ((DirectBuffer)map).cleaner().clean();
//                System.out.println(System.currentTimeMillis() - start);
//            Thread.sleep(1000000);
//            }
//        }
//            for (int i = 0; i < buffers.totalSize(); i++) {
//                ByteBuffer buffer =  buffers.get(i);
//                ((DirectBuffer)buffer).cleaner().clean();
//
//
//            }

//
//        catch (Exception e) {
//            e.printStackTrace();
//
//        }
//        write();
//        try {
//
//            Files.list(Paths.get("/Users/yanbo.liang/test")).forEach(x -> {
//                read(x);
//            });
//
//        } catch (Exception e) {
//            e.printStackTrace();
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
