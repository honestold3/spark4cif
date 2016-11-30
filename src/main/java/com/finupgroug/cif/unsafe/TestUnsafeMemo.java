package com.finupgroug.cif.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by wq on 2016/11/30.
 */
public class TestUnsafeMemo {

    // -XX:MaxDirectMemorySize=40M
    public static void main(String[] args) throws Exception{
        //Unsafe unsafe = Unsafe.getUnsafe();
        Unsafe unsafe = getUnsafe();

        while (true){
            long pointer = unsafe.allocateMemory(1024 * 1024 * 40);
            System.out.println(unsafe.getByte(pointer + 1));

            // 如果不释放内存,运行一段时间会报错java.lang.OutOfMemoryError
            // unsafe.freeMemory(pointer);
        }
    }


    public static Unsafe getUnsafe() {
        try {

            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            return (Unsafe) singleoneInstanceField.get(null);
        } catch (NoSuchFieldException e) {
            throw new NoSuchFieldError(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new IllegalAccessError(e.getMessage());
        }
    }

    public static Unsafe getUnsafeInstance() {
        try{
            // 通过反射获取rt.jar下的Unsafe类
            Field theUnsafeInstance = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeInstance.setAccessible(true);
            // return (Unsafe) theUnsafeInstance.get(null);是等价的
            return (Unsafe) theUnsafeInstance.get(Unsafe.class);
        } catch (NoSuchFieldException e){
            throw new NoSuchFieldError(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new IllegalAccessError(e.getMessage());
        }

    }

}
