package io.openmessaging;

public class Constants {

//    public static int Message_Size = 24;
//    public static int Message_No_T_Size = 16;
//    public static int Thread_Count = 12;
//    public static String Message_Path = "/Users/yanbo.liang/test/message";
//    public static String A_Path = "/Users/yanbo.liang/test/header";

    public static int Message_Size = 50;
    public static int Message_No_T_Size = 42;
    public static int Thread_Count = 12;
    public static String Message_Path = "/alidata1/race2019/data/message";
    public static String A_Path = "/alidata1/race2019/data/header";

    public static int Message_Batch_Size = 4000000;
    public static int Message_Buffer_Size = Message_Batch_Size * Message_Size;

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Compressed_Buffer_Size = 500 * 1024 * 1024;
    static long Write_Message_Buffer_Size = (Message_Size - 8) * Message_Batch_Size;
    static long Write_Header_Buffer_Size = 8 * Message_Batch_Size;
    static long Read_Buffer_Size = 20L * 1024 * 1024;
}
