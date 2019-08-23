package io.openmessaging;

public class Constants {


//    static int Message_Size = 24;
//    static int Message_Long_size = 3;
//    static String Message_Path = "/Users/yanbo.liang/test/message";
//    static String A_Path = "/Users/yanbo.liang/test/header";

    static int Message_Size = 50;
    static int Message_Long_size = 7;
    static String Message_Path = "/alidata1/race2019/data/message";
    static String A_Path = "/alidata1/race2019/data/header";

    static int Message_Batch_Size = 4000000;
    static int Message_Buffer_Size = Message_Batch_Size * Message_Long_size;

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Compressed_Buffer_Size = 500 * 1024 * 1024;
    static long Message_Write_Buffer_Size = 50 * Message_Batch_Size;
    static long Header_Write_Buffer_Size = 8 * Message_Batch_Size;

    static long Direct_Read_Buffer_Size = 20L * 1024 * 1024;
}
