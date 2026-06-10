package org.wowtools.hppt.common.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 传输层重连退避器
 */
public class ReconnectBackoff {
    private final long baseDelayMillis;
    private final long maxDelayMillis;
    private final long jitterMillis;

    private int consecutiveFailures;

    public ReconnectBackoff(long baseDelayMillis, long maxDelayMillis, long jitterMillis) {
        this.baseDelayMillis = Math.max(1L, baseDelayMillis);
        this.maxDelayMillis = Math.max(this.baseDelayMillis, maxDelayMillis);
        this.jitterMillis = Math.max(0L, jitterMillis);
    }

    public long nextDelayMillis() {
        int nextAttempt = consecutiveFailures + 1;
        int exponent = Math.min(nextAttempt - 1, 10);
        long delay = baseDelayMillis;
        for (int i = 0; i < exponent && delay < maxDelayMillis; i++) {
            delay = Math.min(maxDelayMillis, delay * 2);
        }
        consecutiveFailures = nextAttempt;
        if (jitterMillis > 0) {
            delay += ThreadLocalRandom.current().nextLong(jitterMillis + 1);
        }
        return delay;
    }

    public void reset() {
        consecutiveFailures = 0;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }
}
