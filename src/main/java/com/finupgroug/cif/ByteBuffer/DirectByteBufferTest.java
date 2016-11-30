package com.finupgroug.cif.ByteBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by wq on 2016/11/30.
 */

//测试用例1：设置JVM参数-Xmx100m，运行异常，因为如果没设置-XX:MaxDirectMemorySize，则默认与-Xmx参数值相同，分配128M直接内存超出限制范围。

//测试用例2：设置JVM参数-Xmx256m，运行正常，因为128M小于256M，属于范围内分配。

//测试用例3：设置JVM参数-Xmx256m -XX:MaxDirectMemorySize=100M，运行异常，分配的直接内存128M超过限定的100M。
public class DirectByteBufferTest {

    public static void main(String[] args) throws InterruptedException{
        //分配128MB直接内存
        ByteBuffer bb = ByteBuffer.allocateDirect(1024*1024*128);

        TimeUnit.SECONDS.sleep(10);

        System.out.println("ok");
    }

}
