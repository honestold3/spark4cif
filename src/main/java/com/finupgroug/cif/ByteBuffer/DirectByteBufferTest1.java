package com.finupgroug.cif.ByteBuffer;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by wq on 2016/11/30.
 */

//测试用例：设置JVM参数-Xmx768m，运行程序观察内存使用变化，会发现clean()后内存马上下降，说明使用clean()方法能有效及时回收直接缓存。
public class DirectByteBufferTest1 {

    public static void main(String[] args) throws InterruptedException{
        //分配512MB直接缓存
        ByteBuffer bb = ByteBuffer.allocateDirect(1024*1024*512);

        TimeUnit.SECONDS.sleep(10);

        //清除直接缓存
        ((DirectBuffer)bb).cleaner().clean();

        TimeUnit.SECONDS.sleep(10);

        System.out.println("ok");
    }

}
