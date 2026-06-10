package org.wowtools.hppt.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * 缓冲池，内置一个ConcurrentLinkedQueue,用以解耦生产者和消费者、缓冲数据并做监控
 *
 * @author liuyu
 * @date 2024/10/27
 */
@Slf4j
public class BufferPool<T> {
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);

    private final String name;

    // 跟踪等待中的消费者线程，add时主动唤醒
    private volatile Thread waiter;

    /**
     * @param name 缓冲池名字，为便于排查，请保证名称在业务层面的准确清晰
     */
    public BufferPool(String name) {
        this.name = name;
    }

    /**
     * 添加
     *
     * @param t t
     */
    public void add(T t) {
        queue.add(t);
        int n1 = size.incrementAndGet();
        if (DebugConfig.OpenBufferPoolDetector && n1 == DebugConfig.BufferPoolWaterline) {
            log.debug("{} 缓冲池高水位线: {}", name, n1);
        }
        // 主动唤醒等待中的消费者
        Thread w = waiter;
        if (w != null) {
            LockSupport.unpark(w);
        }
    }

    /**
     * 获取,队列为空则一直阻塞等待
     *
     * @return t
     */
    public T take() {
        while (true) {
            T t = queue.poll();
            if (t != null) {
                size.decrementAndGet();
                return t;
            }
            waiter = Thread.currentThread();
            // 双检：注册waiter后再检查一次，防止add()在注册前已执行
            t = queue.poll();
            if (t != null) {
                size.decrementAndGet();
                waiter = null;
                return t;
            }
            LockSupport.park();
            waiter = null;
        }
    }

    /**
     * 获取,队列为空则返回null
     *
     * @return t or null
     */
    public T poll() {
        T t = queue.poll();
        if (t != null) {
            size.decrementAndGet();
        }
        return t;
    }

    /**
     * 获取,队列为空则阻塞等待一段时间,超时则返回null
     *
     * @param timeout timeout
     * @param unit    TimeUnit
     * @return t or null
     */
    public T poll(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            T t = queue.poll();
            if (t != null) {
                size.decrementAndGet();
                return t;
            }
            if (System.nanoTime() >= deadline) {
                return null;
            }
            waiter = Thread.currentThread();
            t = queue.poll();
            if (t != null) {
                size.decrementAndGet();
                waiter = null;
                return t;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                waiter = null;
                return null;
            }
            LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
            waiter = null;
        }
    }

    /**
     * 获取队列中当前可用的所有元素,队列为空则阻塞等待，所以list至少会有一个元素
     *
     * @return list
     */
    public List<T> takeAndDrainToList() {
        List<T> list = new ArrayList<>();
        T t0 = take();
        list.add(t0);
        drainToList(list);
        return list;
    }

    /**
     * 获取队列中当前可用的所有元素，队列为空则返回null
     *
     * @return list
     */
    public List<T> drainToList() {
        if (isEmpty()) {
            return null;
        }
        List<T> list = new ArrayList<>();
        drainToList(list);
        return list;
    }

    /**
     * 获取队列中当前可用的所有元素添加到list中
     *
     * @param list list
     */
    public void drainToList(List<T> list) {
        int drained = 0;
        T t;
        while ((t = queue.poll()) != null) {
            list.add(t);
            drained++;
        }
        if (drained > 0) {
            size.addAndGet(-drained);
        }
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    public int size() {
        return size.get();
    }

    public void clear() {
        queue.clear();
        size.set(0);
    }
}
