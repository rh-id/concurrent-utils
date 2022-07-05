package co.rh.id.lib.concurrent_utils.concurrent.executor;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class WeightedThreadPoolTest {

    private WeightedThreadPool weightedThreadPool;

    @BeforeEach
    void setup() {
        weightedThreadPool = new WeightedThreadPool();
    }

    @Test
    void executorWorks() throws InterruptedException {
        Runnable mockRunnable = Mockito.mock(Runnable.class);
        Runnable mockRunnable2 = Mockito.mock(Runnable.class);
        Executor executor = weightedThreadPool;
        executor.execute(mockRunnable);
        executor.execute(mockRunnable2);
        Thread.sleep(50); // Wait a while
        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);

        Mockito.verify(mockRunnable, Mockito.times(1)).run();
        Mockito.verify(mockRunnable2, Mockito.times(1)).run();
    }

    @Test
    void executorService_submit_works() throws Exception {
        Runnable mockRunnable1 = Mockito.mock(Runnable.class);
        Runnable mockRunnable2 = Mockito.mock(Runnable.class);
        Callable mockCallable = Mockito.mock(Callable.class);
        ExecutorService executorService = weightedThreadPool;
        executorService.submit(mockRunnable1);
        executorService.submit(mockRunnable2, null);
        executorService.submit(mockCallable);
        Thread.sleep(50); // Wait a while
        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);
        Mockito.verify(mockRunnable1, Mockito.times(1)).run();
        Mockito.verify(mockRunnable2, Mockito.times(1)).run();
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    void executorService_invokeAll_works() throws Exception {
        Callable<Object> mockCallable = Mockito.mock(Callable.class);
        Callable<Object> mockCallable2 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable3 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable4 = Mockito.mock(Callable.class);
        ExecutorService executorService = weightedThreadPool;
        executorService.invokeAll(Arrays.asList(mockCallable, mockCallable2));
        executorService.invokeAll(new ArrayList<>(Arrays.asList(mockCallable3, mockCallable4)), 1, TimeUnit.SECONDS);
        Mockito.verify(mockCallable, Mockito.times(1)).call();
        Mockito.verify(mockCallable2, Mockito.times(1)).call();
        Mockito.verify(mockCallable3, Mockito.times(1)).call();
        Mockito.verify(mockCallable4, Mockito.times(1)).call();

        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void executorService_invokeAny_works() throws Exception {
        Callable<Object> mockCallable = Mockito.mock(Callable.class);
        Callable<Object> mockCallable2 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable3 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable4 = Mockito.mock(Callable.class);
        ExecutorService executorService = weightedThreadPool;
        executorService.invokeAny(Arrays.asList(mockCallable, mockCallable2));
        executorService.invokeAny(new ArrayList<>(Arrays.asList(mockCallable3, mockCallable4)), 1, TimeUnit.SECONDS);
        Mockito.verify(mockCallable, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable2, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable3, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable4, Mockito.atMost(1)).call();

        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);
    }
}
