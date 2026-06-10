package org.wowtools.hppt.common.util;

/**
 * I/O线程工具类，用于创建长生命周期的数据转发平台线程
 *
 * @author liuyu
 */
public final class IoThreadUtil {

    private IoThreadUtil() {
    }

    /**
     * 启动一个守护平台线程，用于长生命周期的I/O操作（socket读写等）。
     * 相比虚拟线程，平台线程在长连接I/O场景下无调度开销。
     *
     * @param task 任务
     * @param name 线程名
     * @return 已启动的线程
     */
    public static Thread startIoThread(Runnable task, String name) {
        Thread t = new Thread(task);
        t.setName(name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
