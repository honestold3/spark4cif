package com.finupgroug.cif.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by wq on 2016/11/30.
 */
public class RevisedObjectInHeap {

    private long address = 0;

    private Unsafe unsafe = TestUnsafeMemo.getUnsafeInstance();

    // 让对象占用堆内存,触发[Full GC
    private byte[] bytes = null;

    public RevisedObjectInHeap(){
        address = unsafe.allocateMemory(2 * 1024 * 1024);
        bytes = new byte[1024 * 1024];
    }

    @Override
    protected void finalize() throws Throwable{
        super.finalize();
        System.out.println("finalize." + bytes.length);
        unsafe.freeMemory(address);
    }

    public static void main(String[] args){
        while (true)
        {
            RevisedObjectInHeap heap = new RevisedObjectInHeap();
            System.out.println("memory address=" + heap.address);
        }
    }

}
