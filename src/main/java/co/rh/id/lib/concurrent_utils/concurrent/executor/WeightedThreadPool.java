package co.rh.id.lib.concurrent_utils.concurrent.executor;

import co.rh.id.lib.concurrent_utils.concurrent.WeightedFutureTask;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread pool that distribute the tasks based on the weight of the task.
 * Every thread created will have its own task queue and maximum weight of the tasks that a thread can handle.
 * Default maximum weight is 3 and every default submitted task will have weight of 1, so every thread handle maximum 3 tasks in their queue.
 * This thread pool will attempt to steal tasks if the tasks stays in the thread queue for some time.
 */
@SuppressWarnings("rawtypes")
public class WeightedThreadPool implements ExecutorService {

    private PriorityBlockingQueue<WeightedFutureTask> taskQueue;
    private int maxWeight;
    private long threadTimeoutMillis;
    private AtomicBoolean isShutdown;

    private SchedulerWorker schedulerWorker;

    public WeightedThreadPool() {
        taskQueue = new PriorityBlockingQueue<>();
        maxWeight = 3;
        threadTimeoutMillis = 30_000;
        isShutdown = new AtomicBoolean(false);
        schedulerWorker = new SchedulerWorker();
        schedulerWorker.start();
    }

    public int getMaxWeight() {
        return maxWeight;
    }

    public void setMaxWeight(int maxWeight) {
        this.maxWeight = maxWeight;
    }

    public long getThreadTimeoutMillis() {
        return threadTimeoutMillis;
    }

    public void setThreadTimeoutMillis(long threadTimeoutMillis) {
        this.threadTimeoutMillis = threadTimeoutMillis;
    }

    /**
     * Submit weighted task to this thread pool to be executed
     */
    public <T> WeightedFutureTask<T> submit(Callable<T> callable, int weight) {
        throwIfShutdown();
        WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(callable, weight);
        taskQueue.add(weightedFutureTask);
        return weightedFutureTask;
    }

    /**
     * Submit weighted task to this thread pool to be executed
     */
    public <T> WeightedFutureTask<T> submit(Runnable runnable, T t, int weight) {
        throwIfShutdown();
        WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(runnable, t, weight);
        taskQueue.add(weightedFutureTask);
        return weightedFutureTask;
    }

    @Override
    public void shutdown() {
        if (isShutdown.get()) return;
        isShutdown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> resultList = new ArrayList<>();
        if (isShutdown.get()) return resultList;
        isShutdown.set(true);
        List<WeightedFutureTask> weightedFutureTasks = schedulerWorker.getCurrentTasks();
        if (!weightedFutureTasks.isEmpty()) {
            for (WeightedFutureTask weightedFutureTask : weightedFutureTasks) {
                resultList.add(weightedFutureTask);
            }
        }
        schedulerWorker.cleanUp();
        return resultList;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return isShutdown() && !schedulerWorker.isRunning.get();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        long waitDuration = timeUnit.toMillis(l);
        long startWaitTime = System.currentTimeMillis();
        long totalWaitDuration = 0;
        while (totalWaitDuration < waitDuration) {
            Thread.sleep(schedulerWorker.periodicCheckMilis);
            if (isTerminated()) break;
            totalWaitDuration = System.currentTimeMillis() - startWaitTime;
        }
        return isTerminated();
    }

    @Override
    public <T> WeightedFutureTask<T> submit(Callable<T> callable) {
        throwIfShutdown();
        WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(callable);
        taskQueue.add(weightedFutureTask);
        return weightedFutureTask;
    }

    @Override
    public <T> WeightedFutureTask<T> submit(Runnable runnable, T t) {
        throwIfShutdown();
        WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(runnable, t);
        taskQueue.add(weightedFutureTask);
        return weightedFutureTask;
    }

    @Override
    public WeightedFutureTask<?> submit(Runnable runnable) {
        throwIfShutdown();
        WeightedFutureTask<?> weightedFutureTask = new WeightedFutureTask<>(runnable, null);
        taskQueue.add(weightedFutureTask);
        return weightedFutureTask;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) throws InterruptedException {
        return invokeMultiple(collection, null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException {
        return invokeMultiple(collection, timeUnit.toMillis(l));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection) throws InterruptedException, ExecutionException {
        return invokeOne(collection, null);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeOne(collection, timeUnit.toMillis(l));
    }

    @Override
    public void execute(Runnable runnable) {
        throwIfShutdown();
        WeightedFutureTask<?> weightedFutureTask = new WeightedFutureTask<>(runnable, null);
        taskQueue.add(weightedFutureTask);
    }

    private <T> List<Future<T>> invokeMultiple(Collection<? extends Callable<T>> collection, Long waitTime) {
        throwIfShutdown();
        List<Future<T>> futureList = new ArrayList<>();
        if (!collection.isEmpty()) {
            for (Callable<T> callable : collection) {
                WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(callable);
                taskQueue.add(weightedFutureTask);
                futureList.add(weightedFutureTask);
            }
        }
        if (!futureList.isEmpty()) {
            // Wait until ALL task finish
            long startWaitTime = System.currentTimeMillis();
            boolean tasksDone = true;
            do {
                for (Future<T> future : futureList) {
                    if (!future.isDone()) {
                        tasksDone = false;
                        break;
                    } else {
                        tasksDone = true;
                    }
                }
                long waitDuration = System.currentTimeMillis() - startWaitTime;
                if (waitTime != null) {
                    if (waitDuration >= waitTime) {
                        tasksDone = true;
                    }
                }
            } while (!tasksDone);
        }
        return futureList;
    }

    private <T> T invokeOne(Collection<? extends Callable<T>> collection, Long waitTime) throws ExecutionException, InterruptedException {
        throwIfShutdown();
        List<Future<T>> futureList = new ArrayList<>();
        if (!collection.isEmpty()) {
            for (Callable<T> callable : collection) {
                WeightedFutureTask<T> weightedFutureTask = new WeightedFutureTask<>(callable);
                taskQueue.add(weightedFutureTask);
                futureList.add(weightedFutureTask);
            }
        }
        if (!futureList.isEmpty()) {
            // Wait until ONE task finish
            long startWaitTime = System.currentTimeMillis();
            boolean tasksDone = false;
            Future<T> result = null;
            do {
                for (Future<T> future : futureList) {
                    if (future.isDone()) {
                        result = future;
                        tasksDone = true;
                        break;
                    }
                }
                long waitDuration = System.currentTimeMillis() - startWaitTime;
                if (waitTime != null) {
                    if (waitDuration >= waitTime) {
                        tasksDone = true;
                    }
                }
            } while (!tasksDone);
            if (result != null) {
                for (Future<T> future : futureList) {
                    future.cancel(true);
                }
                return result.get();
            }
            return null;
        }
        throw new ExecutionException("No task completed", null);
    }

    private void throwIfShutdown() {
        if (isShutdown.get()) throw new RejectedExecutionException("Thread pool has shutdown");
    }

    private class SchedulerWorker extends Thread {
        private long periodicCheckMilis;
        private List<Worker> activeThreads;
        private AtomicBoolean isRunning;

        public SchedulerWorker() {
            setDaemon(true);
            periodicCheckMilis = 17;
            activeThreads = Collections.synchronizedList(new ArrayList<>());
            isRunning = new AtomicBoolean(false);
        }

        // Remove thread from active thread and thread tasks map after worker finish or timeout
        private synchronized void workerFinish(Worker worker) {
            if (worker.workerTaskQueue.isEmpty()) {
                activeThreads.remove(worker);
                Collections.sort(activeThreads);
            }
        }

        private synchronized void addWorker(WeightedFutureTask weightedFutureTask) {
            Worker worker = new Worker();
            worker.add(weightedFutureTask);
            worker.start();
            activeThreads.add(worker);
            Collections.sort(activeThreads);
        }

        private synchronized void addWorker(List<WeightedFutureTask> weightedFutureTasks) {
            for (WeightedFutureTask weightedFutureTask : weightedFutureTasks) {
                Worker worker = new Worker();
                worker.add(weightedFutureTask);
                worker.start();
                activeThreads.add(worker);
            }
            Collections.sort(activeThreads);
        }

        private synchronized void assignTask(WeightedFutureTask weightedFutureTask) {
            if (activeThreads.isEmpty() || weightedFutureTask.getWeight() >= maxWeight) {
                addWorker(weightedFutureTask);
            } else {
                boolean assigned = false;
                for (Worker worker : activeThreads) {
                    if (worker.getTasksWeight() < maxWeight) {
                        worker.add(weightedFutureTask);
                        assigned = true;
                        break;
                    }
                }
                if (!assigned) {
                    addWorker(weightedFutureTask);
                }
            }
        }

        @Override
        public synchronized void start() {
            isRunning.set(true);
            super.start();
        }

        @Override
        public void run() {
            while (!isShutdown()) {
                WeightedFutureTask task = null;
                try {
                    task =
                            taskQueue.poll(periodicCheckMilis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    // Nothing to do when interrupted
                }
                if (task != null) {
                    assignTask(task);
                }
                // check deadlock and tasks that waits too long
                if (!activeThreads.isEmpty()) {
                    for (Worker worker : activeThreads) {
                        List<WeightedFutureTask> taskList = worker.stealTasks(periodicCheckMilis);
                        // assigning new thread to break deadlock and to let it execute immediately
                        if (!taskList.isEmpty()) {
                            addWorker(taskList);
                        }
                    }
                }
            }
            isRunning.set(false);
        }

        public void cleanUp() {
            boolean done = false;
            while (!done) {
                if (!isRunning.get()) {
                    if (!activeThreads.isEmpty()) {
                        for (Worker worker : activeThreads) {
                            worker.cleanUp();
                        }
                    }
                    done = true;
                } else {
                    try {
                        Thread.sleep(periodicCheckMilis);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                }
            }
        }

        public List<WeightedFutureTask> getCurrentTasks() {
            List<WeightedFutureTask> resultList = new ArrayList<>();
            if (!activeThreads.isEmpty()) {
                for (Worker worker : activeThreads) {
                    if (!worker.workerTaskQueue.isEmpty()) {
                        List<WeightedFutureTask> tasks = new ArrayList<>();
                        worker.workerTaskQueue.drainTo(tasks);
                        resultList.addAll(tasks);
                    }
                }
            }
            return resultList;
        }

        private class Worker extends Thread implements Comparable<Worker> {

            private final BlockingQueue<WeightedFutureTask> workerTaskQueue;
            /**
             * A map of task as key and time millis as value to store the time of task added to queue
             */
            private final Map<WeightedFutureTask, Long> taskAssignedMap;

            public Worker() {
                setDaemon(false);
                workerTaskQueue = new PriorityBlockingQueue<>();
                taskAssignedMap = new ConcurrentHashMap<>();
            }

            @Override
            public void run() {
                WeightedFutureTask task = null;
                do {
                    try {
                        task = workerTaskQueue.poll(threadTimeoutMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        // Do nothing
                    }
                    if (task != null) {
                        taskAssignedMap.remove(task);
                        if (!task.hasRun()) {
                            task.run();
                        }
                    }
                    if (isShutdown()) break;
                } while (task != null);
                workerFinish(this);
            }

            private int getTasksWeight() {
                Iterator<WeightedFutureTask> iterator = workerTaskQueue.iterator();
                int totalWeight = 0;
                while (iterator.hasNext()) {
                    WeightedFutureTask weightedFutureTask = iterator.next();
                    totalWeight += weightedFutureTask.getWeight();
                }
                return totalWeight;
            }

            @Override
            public int compareTo(Worker worker) {
                int currentWeight = getTasksWeight();
                int thatWeight = worker.getTasksWeight();
                if (currentWeight < thatWeight) {
                    return -1;
                } else if (currentWeight > thatWeight) {
                    return +1;
                }
                return 0;
            }

            private void add(WeightedFutureTask weightedFutureTask) {
                workerTaskQueue.add(weightedFutureTask);
                taskAssignedMap.put(weightedFutureTask, System.currentTimeMillis());
            }

            /**
             * Steal tasks that wait too long to be executed
             */
            private List<WeightedFutureTask> stealTasks(long waitMilis) {
                List<WeightedFutureTask> resultList = new ArrayList<>();
                if (!taskAssignedMap.isEmpty()) {
                    long currentTime = System.currentTimeMillis();
                    Iterator<Map.Entry<WeightedFutureTask, Long>> iterator = taskAssignedMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<WeightedFutureTask, Long> entry = iterator.next();
                        if (currentTime - entry.getValue() >= waitMilis) {
                            WeightedFutureTask weightedFutureTask = entry.getKey();
                            boolean removed = workerTaskQueue.remove(weightedFutureTask);
                            if (removed) {
                                resultList.add(weightedFutureTask);
                                iterator.remove();
                            }
                        }
                    }
                }
                return resultList;
            }

            public void cleanUp() {
                if (isShutdown() && !workerTaskQueue.isEmpty()) {
                    workerTaskQueue.clear();
                }
            }
        }
    }
}
