package org.wowtools.hppt.common.util;

/**
 * 粗粒度的时间戳获取工具，由于系统频繁地System.currentTimeMillis()，做一个定时器统一获取时间减少性能开销
 * 5秒更新一次时间
 *
 * @author liuyu
 * @date 2023/3/7
 */
public class RoughTimeUtil {
    private static volatile long timestamp = System.currentTimeMillis();
    private static volatile boolean running = true;

    static {
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
                timestamp = System.currentTimeMillis();
            }
        });
    }

    public static void stop() {
        running = false;
    }

    public static long getTimestamp() {
        return timestamp;
    }
}
