package io.openmessaging;

public class Constants {

    static int Message_Size = 24;
    static String Message_Path = "/Users/yanbo.liang/test/message";
    static String Header_Path = "/Users/yanbo.liang/test/header";

//    static int Message_Size = 50;
//    static String Message_Path = "/alidata1/race2019/data/message";
//    static String Header_Path = "/alidata1/race2019/data/header";

    static int Message_Batch_Size = 5000000;

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Compressed_Buffer_Size = 1024 * 1024 * 1024;
    static long Message_Write_Buffer_Size = Message_Size * Message_Batch_Size;
    static long Header_Write_Buffer_Size = 8 * Message_Batch_Size;

    static long Direct_Read_Buffer_Size = 40L * 1024 * 1024;

}
