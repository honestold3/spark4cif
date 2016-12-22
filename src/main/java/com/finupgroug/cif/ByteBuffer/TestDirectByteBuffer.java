package com.finupgroug.cif.ByteBuffer;

import java.nio.ByteBuffer;

/**
 * Created by wq on 2016/11/25.
 */
public class TestDirectByteBuffer {

    // -verbose:gc -XX:+PrintGCDetails -XX:MaxDirectMemorySize=40M

    // -verbose:gc -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:MaxDirectMemorySize=40M

    public static void main(String[] args) throws Exception
    {
        while (true){
            ByteBuffer buffer = ByteBuffer.allocateDirect(10 * 1024 * 1024);
        }
    }

}
