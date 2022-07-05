package co.rh.id.lib.concurrent_utils.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FutureTask implementation with weight value assigned to it, default weight value is 1
 */
public class WeightedFutureTask<RESULT> extends FutureTask<RESULT> implements Comparable<WeightedFutureTask> {

    private int weight = 1;
    private AtomicBoolean hasRun = new AtomicBoolean(false);

    public WeightedFutureTask(Callable<RESULT> callable) {
        super(callable);
    }

    public WeightedFutureTask(Runnable runnable, RESULT result) {
        super(runnable, result);
    }

    public WeightedFutureTask(Callable<RESULT> callable, int weight) {
        this(callable);
        this.weight = weight;
    }

    public WeightedFutureTask(Runnable runnable, RESULT result, int weight) {
        this(runnable, result);
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public boolean hasRun() {
        return hasRun.get();
    }

    @Override
    public int compareTo(WeightedFutureTask weightedFutureTask) {
        if (this.weight > weightedFutureTask.weight) {
            return -1;
        } else if (this.weight < weightedFutureTask.weight) {
            return +1;
        }
        return 0;
    }

    @Override
    public void run() {
        hasRun.set(true);
        super.run();
    }
}
