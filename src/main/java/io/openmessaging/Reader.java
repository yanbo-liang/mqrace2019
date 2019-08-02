package io.openmessaging;

import java.io.File;

public class Reader {


    public void getAllFiles() {
        File file = new File("/Users/yanbo.liang/test/");
        File[] files = file.listFiles();
    }
}
