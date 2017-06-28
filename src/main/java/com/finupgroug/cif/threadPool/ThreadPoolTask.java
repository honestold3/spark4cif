package com.finupgroug.cif.threadPool;

/**
 * Created by wq on 2017/6/10.
 */
public class ThreadPoolTask implements Runnable {
    // 保存数据
    private Object threadPoolTaskData;

    private static int consumerTaskSleepTime = 2000;

    ThreadPoolTask(Object tasks) {
        this.threadPoolTaskData = tasks;
    }

    public void run() {
        // 处理
        System.out.println("start .." + threadPoolTaskData);
        try {

            Thread.sleep(consumerTaskSleepTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("finish " + threadPoolTaskData);
        threadPoolTaskData = null;
    }

}