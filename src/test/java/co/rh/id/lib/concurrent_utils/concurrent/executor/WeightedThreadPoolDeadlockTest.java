package co.rh.id.lib.concurrent_utils.concurrent.executor;


import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WeightedThreadPoolDeadlockTest {

    // This scenario was from a-personal-stuff project: ItemFileHelper.java
    @Test
    void nestedFutureTaskScenario() throws Exception {
        Callable<Object> callable1 = Mockito.spy(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                Thread.sleep(1000);
                return null;
            }
        });
        WeightedThreadPool weightedThreadPool = new WeightedThreadPool();
        ExecutorService executorService = weightedThreadPool;
        Future<Object> task1 = executorService.submit(
                callable1
        );
        Callable<Boolean> callable2 = Mockito.spy(
                new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        task1.get();
                        Thread.sleep(500);
                        return true;
                    }
                }
        );
        Callable<Boolean> callable3 = Mockito.spy(
                new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        Thread.sleep(500);
                        return true;
                    }
                }
        );
        Runnable runnable1 = Mockito.spy(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            List<Future<Boolean>> taskList = new ArrayList<>();
                            taskList.add(
                                    executorService.submit(callable2)
                            );
                            taskList.add(
                                    executorService.submit(callable3)
                            );
                            for (Future<Boolean> task : taskList) {
                                task.get();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
        executorService.execute(runnable1);
        Thread.sleep(1100); // Wait a while
        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);
        Mockito.verify(callable1, Mockito.times(1)).call();
        Mockito.verify(callable2, Mockito.times(1)).call();
        Mockito.verify(callable3, Mockito.times(1)).call();
        Mockito.verify(runnable1, Mockito.times(1)).run();
    }
}
