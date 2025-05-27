package com.netflix.loadbalancer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.config.DynamicIntProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自旋拉取服务列表更新器
 *
 * A default strategy for the dynamic server list updater to update.
 * (refactored and moved here from {@link com.netflix.loadbalancer.DynamicServerListLoadBalancer})
 *
 * @author David Liu
 */
public class PollingServerListUpdater implements ServerListUpdater {

    private static final Logger logger = LoggerFactory.getLogger(PollingServerListUpdater.class);
    // 毫秒
    private static final long LISTOFSERVERS_CACHE_UPDATE_DELAY = 1000; // msecs;
    private static final int LISTOFSERVERS_CACHE_REPEAT_INTERVAL = 30 * 1000; // msecs;

    // 懒加载
    private static class LazyHolder {
        private final static String CORE_THREAD = "DynamicServerListLoadBalancer.ThreadPoolSize";
        // 线程池核心线程数
        private final static DynamicIntProperty poolSizeProp = new DynamicIntProperty(CORE_THREAD, 2);
        private static Thread _shutdownThread;
        // 定时任务线程池
        static ScheduledThreadPoolExecutor _serverListRefreshExecutor = null;
        // 构造线程池
        static {
            int coreSize = poolSizeProp.get();
            ThreadFactory factory = (new ThreadFactoryBuilder())
                    .setNameFormat("PollingServerListUpdater-%d")
                    .setDaemon(true)
                    .build();
            _serverListRefreshExecutor = new ScheduledThreadPoolExecutor(coreSize, factory);
            // 动态修改
            poolSizeProp.addCallback(new Runnable() {
                @Override
                public void run() {
                    _serverListRefreshExecutor.setCorePoolSize(poolSizeProp.get());
                }

            });
            _shutdownThread = new Thread(new Runnable() {
                public void run() {
                    logger.info("Shutting down the Executor Pool for PollingServerListUpdater");
                    shutdownExecutorPool();
                }
            });
            // 关闭线程池沟子方法
            Runtime.getRuntime().addShutdownHook(_shutdownThread);
        }
        private static void shutdownExecutorPool() {
            if (_serverListRefreshExecutor != null) {
                _serverListRefreshExecutor.shutdown();
                if (_shutdownThread != null) {
                    try {
                        Runtime.getRuntime().removeShutdownHook(_shutdownThread);
                    } catch (IllegalStateException ignore) {
                    }
                }
            }
        }
    }

    private static ScheduledThreadPoolExecutor getRefreshExecutor() {
        return LazyHolder._serverListRefreshExecutor;
    }

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private volatile long lastUpdated = System.currentTimeMillis();
    private final long initialDelayMs;
    private final long refreshIntervalMs;

    private volatile ScheduledFuture<?> scheduledFuture;

    public PollingServerListUpdater() {
        this(LISTOFSERVERS_CACHE_UPDATE_DELAY, LISTOFSERVERS_CACHE_REPEAT_INTERVAL);
    }

    public PollingServerListUpdater(IClientConfig clientConfig) {
        this(LISTOFSERVERS_CACHE_UPDATE_DELAY, getRefreshIntervalMs(clientConfig));
    }

    public PollingServerListUpdater(final long initialDelayMs, final long refreshIntervalMs) {
        // 首次延迟执行
        this.initialDelayMs = initialDelayMs;
        // 执行间隔
        this.refreshIntervalMs = refreshIntervalMs;
    }

    @Override
    public synchronized void start(final UpdateAction updateAction) {
        if (isActive.compareAndSet(false, true)) {
            final Runnable wrapperRunnable = new Runnable() {
                @Override
                public void run() {
                    if (!isActive.get()) {
                        if (scheduledFuture != null) {
                            scheduledFuture.cancel(true);
                        }
                        return;
                    }
                    try {
                        // 定时任务主动拉取服务列表
                        updateAction.doUpdate();
                        // 上次执行完成时间
                        lastUpdated = System.currentTimeMillis();
                    } catch (Exception e) {
                        logger.warn("Failed one update cycle", e);
                    }
                }
            };
            // 定时任务执行
            scheduledFuture = getRefreshExecutor().scheduleWithFixedDelay(
                    wrapperRunnable,
                    initialDelayMs,
                    refreshIntervalMs,
                    TimeUnit.MILLISECONDS
            );
        } else {
            logger.info("Already active, no-op");
        }
    }

    @Override
    public synchronized void stop() {
        if (isActive.compareAndSet(true, false)) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        } else {
            logger.info("Not active, no-op");
        }
    }

    @Override
    public String getLastUpdate() {
        return new Date(lastUpdated).toString();
    }

    @Override
    public long getDurationSinceLastUpdateMs() {
        return System.currentTimeMillis() - lastUpdated;
    }

    @Override
    public int getNumberMissedCycles() {
        if (!isActive.get()) {
            return 0;
        }
        return (int) ((int) (System.currentTimeMillis() - lastUpdated) / refreshIntervalMs);
    }

    @Override
    public int getCoreThreads() {
        if (isActive.get()) {
            if (getRefreshExecutor() != null) {
                return getRefreshExecutor().getCorePoolSize();
            }
        }
        return 0;
    }

    private static long getRefreshIntervalMs(IClientConfig clientConfig) {
        return clientConfig.get(CommonClientConfigKey.ServerListRefreshInterval, LISTOFSERVERS_CACHE_REPEAT_INTERVAL);
    }
}
