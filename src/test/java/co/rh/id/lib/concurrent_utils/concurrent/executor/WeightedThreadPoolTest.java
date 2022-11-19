package co.rh.id.lib.concurrent_utils.concurrent.executor;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

    @AfterEach
    void dispose() throws InterruptedException {
        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void executorWorks() throws InterruptedException {
        Runnable mockRunnable = Mockito.mock(Runnable.class);
        Runnable mockRunnable2 = Mockito.mock(Runnable.class);
        Runnable mockRunnable3 = Mockito.mock(Runnable.class);
        Executor executor = weightedThreadPool;
        executor.execute(mockRunnable);
        executor.execute(mockRunnable2);
        weightedThreadPool.execute(mockRunnable3, weightedThreadPool.getMaxWeight());
        Thread.sleep(50); // Wait a while
        weightedThreadPool.shutdown();
        weightedThreadPool.awaitTermination(1, TimeUnit.SECONDS);

        Mockito.verify(mockRunnable, Mockito.times(1)).run();
        Mockito.verify(mockRunnable2, Mockito.times(1)).run();
        Mockito.verify(mockRunnable3, Mockito.times(1)).run();
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
        Callable<Object> mockCallable5 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable6 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable7 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable8 = Mockito.mock(Callable.class);
        ExecutorService executorService = weightedThreadPool;
        executorService.invokeAll(Arrays.asList(mockCallable, mockCallable2));
        executorService.invokeAll(Arrays.asList(mockCallable3, mockCallable4), 1, TimeUnit.SECONDS);
        weightedThreadPool.invokeAll(weightedThreadPool.getMaxWeight(), Arrays.asList(mockCallable5, mockCallable6));
        weightedThreadPool.invokeAll(weightedThreadPool.getMaxWeight(), Arrays.asList(mockCallable7, mockCallable8), 1, TimeUnit.SECONDS);
        Mockito.verify(mockCallable, Mockito.times(1)).call();
        Mockito.verify(mockCallable2, Mockito.times(1)).call();
        Mockito.verify(mockCallable3, Mockito.times(1)).call();
        Mockito.verify(mockCallable4, Mockito.times(1)).call();
        Mockito.verify(mockCallable5, Mockito.times(1)).call();
        Mockito.verify(mockCallable6, Mockito.times(1)).call();
        Mockito.verify(mockCallable7, Mockito.times(1)).call();
        Mockito.verify(mockCallable8, Mockito.times(1)).call();
    }

    @Test
    void executorService_invokeAny_works() throws Exception {
        Callable<Object> mockCallable = Mockito.mock(Callable.class);
        Callable<Object> mockCallable2 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable3 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable4 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable5 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable6 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable7 = Mockito.mock(Callable.class);
        Callable<Object> mockCallable8 = Mockito.mock(Callable.class);
        ExecutorService executorService = weightedThreadPool;
        executorService.invokeAny(Arrays.asList(mockCallable, mockCallable2));
        executorService.invokeAny(Arrays.asList(mockCallable3, mockCallable4), 1, TimeUnit.SECONDS);
        weightedThreadPool.invokeAny(weightedThreadPool.getMaxWeight(), Arrays.asList(mockCallable5, mockCallable6));
        weightedThreadPool.invokeAny(weightedThreadPool.getMaxWeight(), Arrays.asList(mockCallable7, mockCallable8), 1, TimeUnit.SECONDS);
        Mockito.verify(mockCallable, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable2, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable3, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable4, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable5, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable6, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable7, Mockito.atMost(1)).call();
        Mockito.verify(mockCallable8, Mockito.atMost(1)).call();
    }
}
