package io.openmessaging;

public class Constants {

//    public static int Message_Size = 24;
//    public static String Body_Path = "/Users/yanbo.liang/test/body";
//    public static String A_Path = "/Users/yanbo.liang/test/a";
//    public static String Sorted_A_Path = "/Users/yanbo.liang/test/sorted_a";

    public static int Message_Size = 50;
    public static String Body_Path = "/alidata1/race2019/data/body";
    public static String A_Path = "/alidata1/race2019/data/a";
    public static String Sorted_A_Path = "/alidata1/race2019/data/sorted_a";

    public static int Thread_Count = 12;
    public static int Body_Size = Message_Size - 16;
    public static int Batch_Size = 5000000;
    public static long Partition_Size = 2000;


    public static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    public static long Compressed_Buffer_Size = 400 * 1024 * 1024;
    public static long Write_Body_Buffer_Size = Body_Size * Batch_Size;
    public static long Write_A_Buffer_Size = 8 * Batch_Size;
    public static long Write_Sorted_A_Buffer_Size = 7 * Batch_Size;

    public static long A_Mark = (1L << 48) - 1;
}
