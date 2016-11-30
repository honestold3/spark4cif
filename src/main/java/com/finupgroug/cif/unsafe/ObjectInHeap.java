package com.finupgroug.cif.unsafe;

import sun.misc.Unsafe;

/**
 * Created by wq on 2016/11/30.
 */
public class ObjectInHeap {

    private long address = 0;

    private Unsafe unsafe = TestUnsafeMemo.getUnsafe();

    public ObjectInHeap(){
        address = unsafe.allocateMemory(2 * 1024 * 1024);
    }

    // Exception in thread "main" java.lang.OutOfMemoryError
    public static void main(String[] args){
        while (true)
        {
            ObjectInHeap heap = new ObjectInHeap();
            System.out.println("memory address=" + heap.address);
        }
    }

}
