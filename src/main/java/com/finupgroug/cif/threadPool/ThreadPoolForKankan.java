package com.finupgroug.cif.threadPool;

import java.util.concurrent.*;

/**
 * Created by wq on 2017/6/10.
 */
public class ThreadPoolForKankan {

    private static int executePrograms = 0;
    private static int produceTaskMaxNumber = 300;

    public static void main(String[] args){
        // 构造一个线程池

//        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 4, 3,
//                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(3));

//        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
//        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
//        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());

        ExecutorService threadPool = Executors.newFixedThreadPool(1);

        for (int i = 1; i <= produceTaskMaxNumber; i++) {
            try {
                String task = "task@ " + i;
                System.out.println("put " + task);
                threadPool.execute(new ThreadPoolTask(task));

                Thread.sleep(executePrograms);
            } catch (Exception e) {
                //System.out.println("ddd");
                e.printStackTrace();
            }
        }
        threadPool.shutdown();
    }

}
