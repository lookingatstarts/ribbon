/*
*
* Copyright 2013 Netflix, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package com.netflix.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.Server;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;

/**
 *
 * 启动连接操作：通过探测实例的接口，来确定服务是否可以接收服务请求
 *
 * Prime the connections for a given Client (For those Client that
 * have a LoadBalancer that knows the set of Servers it will connect to) This is
 * mainly done to address those deployment environments (Read EC2) which benefit
 * from a firewall connection/path warmup prior to actual use for live requests.
 * <p>
 * This class is not protocol specific. Actual priming operation is delegated to 
 * instance of {@link IPrimeConnection}, which is instantiated using reflection
 * according to property {@link CommonClientConfigKey#PrimeConnectionsClassName}.
 * 
 * @author stonse
 * @author awang
 * @author aspyker
 * 
 */
public class PrimeConnections {

    /**
     * 感知server#readyToServe字段变化
     */
    public static interface PrimeConnectionListener {
        public void primeCompleted(Server s, Throwable lastException);
    }

    /**
     * 统计时间
     */
    public static class PrimeConnectionEndStats {
        //
        public final int total;
        public final int success;
        public final int failure;
        //同步prime connection阻塞时长
        public final long totalTime;

        public PrimeConnectionEndStats(int total, int success, int failure, long totalTime) {
            this.total = total;
            this.success = success;
            this.failure = failure;
            this.totalTime = totalTime;
        }

        @Override
        public String toString() {
            return "PrimeConnectionEndStats [total=" + total + ", success="
                + success + ", failure=" + failure + ", totalTime="
                + totalTime + "]";
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PrimeConnections.class);
    /**
     * 检查URI，默认“/”对大多数人来说都是好事（毕竟我们只需要能连上就行），
     * 但如果服务端对此访问是个重操作额话（比如有些应用返回主页），那请换个轻量级的URI（在Filter里返回个常量最好）
     * 只要能连接上实例就行
     */
    String primeConnectionsURIPath = "/";

    /**
     * 异步线程池执行异步请求
     * 每次拉取到新的server列表时，会调用asyncPrimeConnections，异步进行prime
     */
    private ExecutorService executorService;
    private int maxExecutorThreads = 5;
    /**
     * 线程池线程空闲时间
     */
    private long executorThreadTimeout = 30000;
    /**
     * 服务名称
     */
    private String name = "default";
    /**
     * 同步阻塞prime时使用
     * 放行的比例，eg:你有N台Server，乘以这个比率就是最终多少台完成了（并不代表成功）就不要阻塞主线程了，
     * 默认是100%表示全部完成检测了才会放行（
     * 注意：它只影响阻塞or不阻塞的情况，并不影响每台Server自己的readyToServe属性值，因为此属性值只跟检测结果有关
     *
     * 同步等待多少比例的server执行过prime就放行
     */
    private float primeRatio = 1.0f;
    int maxRetries = 9;
    // 同步prime时，最大的阻塞时长
    long maxTotalTimeToPrimeConnections = 30 * 1000; // default time
    private boolean aSync = true;
    Counter totalCounter;
    Counter successCounter;
    Timer initialPrimeTimer;
    // prime实现方式：HttpPrimeConnection
    private IPrimeConnection connector;
   // 统计信息
    private PrimeConnectionEndStats stats;

    private PrimeConnections() {
    }

    /**
     * @param name 服务名称
     * @param niwsClientConfig 配置
     */
    public PrimeConnections(String name, IClientConfig niwsClientConfig) {
        // 每台实例最大的重试次数，默认9
        int maxRetriesPerServerPrimeConnection = Integer.valueOf(DefaultClientConfigImpl.DEFAULT_MAX_RETRIES_PER_SERVER_PRIME_CONNECTION);
        // primeConnection耗时
        long maxTotalTimeToPrimeConnections = Long.valueOf(DefaultClientConfigImpl.DEFAULT_MAX_TOTAL_TIME_TO_PRIME_CONNECTIONS);
        // prime请求uri
        String primeConnectionsURI = DefaultClientConfigImpl.DEFAULT_PRIME_CONNECTIONS_URI;
        try {
            // key: MaxRetriesPerServerPrimeConnection
            maxRetriesPerServerPrimeConnection = Integer.parseInt(String.valueOf(niwsClientConfig.getProperty(
                    CommonClientConfigKey.MaxRetriesPerServerPrimeConnection, maxRetriesPerServerPrimeConnection)));
        } catch (Exception e) {
            logger.warn("Invalid maxRetriesPerServerPrimeConnection");
        }
        try {
            // key：MaxTotalTimeToPrimeConnections
            maxTotalTimeToPrimeConnections = Long.parseLong(String.valueOf(niwsClientConfig.getProperty(
                    CommonClientConfigKey.MaxTotalTimeToPrimeConnections,maxTotalTimeToPrimeConnections)));
        } catch (Exception e) {
            logger.warn("Invalid maxTotalTimeToPrimeConnections");
        }
        // key: PrimeConnectionsURI
        primeConnectionsURI = String.valueOf(niwsClientConfig.getProperty(CommonClientConfigKey.PrimeConnectionsURI, primeConnectionsURI));
        // 比例
        float primeRatio = Float.parseFloat(String.valueOf(niwsClientConfig.getProperty(CommonClientConfigKey.MinPrimeConnectionsRatio)));
        String className = niwsClientConfig.getPropertyAsString(CommonClientConfigKey.PrimeConnectionsClassName,
                DefaultClientConfigImpl.DEFAULT_PRIME_CONNECTIONS_CLASS);
        try {
            // 默认为HttpPrimeConnection，创建对象，
            connector = (IPrimeConnection) Class.forName(className).newInstance();
            connector.initWithNiwsConfig(niwsClientConfig);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize prime connections", e);
        }
        setUp(name, maxRetriesPerServerPrimeConnection, 
                maxTotalTimeToPrimeConnections, primeConnectionsURI, primeRatio);        
    }
        
    public PrimeConnections(String name, int maxRetries, 
            long maxTotalTimeToPrimeConnections, String primeConnectionsURI) {
        setUp(name, maxRetries, maxTotalTimeToPrimeConnections, primeConnectionsURI, DefaultClientConfigImpl.DEFAULT_MIN_PRIME_CONNECTIONS_RATIO);
    }

    public PrimeConnections(String name, int maxRetries, 
            long maxTotalTimeToPrimeConnections, String primeConnectionsURI, float primeRatio) {
        setUp(name, maxRetries, maxTotalTimeToPrimeConnections, primeConnectionsURI, primeRatio);
    }

    /**
     * 设置
     */
    private void setUp(String name, int maxRetries, 
            long maxTotalTimeToPrimeConnections, String primeConnectionsURI, float primeRatio) {        
        this.name = name;
        this.maxRetries = maxRetries;
        this.maxTotalTimeToPrimeConnections = maxTotalTimeToPrimeConnections;
        this.primeConnectionsURIPath = primeConnectionsURI;        
        this.primeRatio = primeRatio;
        // 异步线程池
        executorService = new ThreadPoolExecutor(1,
                maxExecutorThreads, executorThreadTimeout, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ASyncPrimeConnectionsThreadFactory(name)
        );
        // 数据采集和监控
        totalCounter = Monitors.newCounter(name + "_PrimeConnection_TotalCounter");
        successCounter = Monitors.newCounter(name + "_PrimeConnection_SuccessCounter");
        initialPrimeTimer = Monitors.newTimer(name + "_initialPrimeConnectionsTimer", TimeUnit.MILLISECONDS);
        Monitors.registerObject(name + "_PrimeConnection", this);
    }
    
    /**
     *
     * 同步prime connection完成，阻塞默认最长30s
     * 
     */
    public void primeConnections(List<Server> servers) {
        if (servers == null || servers.size() == 0) {
            logger.debug("No server to prime");
            return;
        }
        // readyToServe设置为false,负载均衡器不会选择这个实例
        for (Server server: servers) {
            server.setReadyToServe(false);
        }
        // 比例默认1，即100%
        int totalCount = (int) (servers.size() * primeRatio);
        // 等待prime完成
        final CountDownLatch latch = new CountDownLatch(totalCount);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failureCount= new AtomicInteger(0);
        primeConnectionsAsync(servers, new PrimeConnectionListener()  {            
            @Override
            public void primeCompleted(Server s, Throwable lastException) {
                if (lastException == null) {
                    successCount.incrementAndGet();
                    s.setReadyToServe(true);
                } else {
                    failureCount.incrementAndGet();
                }
                latch.countDown();
            }
        });
        Stopwatch stopWatch = initialPrimeTimer.start();
        try {
            // 阻塞等待prime完成，primeConnection最大阻塞时间30s
            latch.await(maxTotalTimeToPrimeConnections, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Priming connection interrupted", e);
        } finally {
            stopWatch.stop();
        }
        // 打印统计信息
        stats = new PrimeConnectionEndStats(totalCount, successCount.get(), failureCount.get(), stopWatch.getDuration(TimeUnit.MILLISECONDS));
        printStats(stats);
    }

    public PrimeConnectionEndStats getEndStats() {
        return stats;
    }

    private void printStats(PrimeConnectionEndStats stats) {
        if (stats.total != stats.success) {
            logger.info("Priming Connections not fully successful");
        } else {
            logger.info("Priming connections fully successful");
        }
        logger.debug("numServers left to be 'primed'="
                + (stats.total - stats.success));
        logger.debug("numServers successfully 'primed'=" + stats.success);
        logger.debug("numServers whose attempts not complete exclusively due to max time allocated="
                        + (stats.total - (stats.success + stats.failure)));
        logger.debug("Total Time Taken=" + stats.totalTime
                + " msecs, out of an allocated max of (msecs)="
                + maxTotalTimeToPrimeConnections);
        logger.debug("stats = " + stats);
    }

    /**
     * Prime servers asynchronously.
     * 异步prime可用
     */
    public List<Future<Boolean>> primeConnectionsAsync(final List<Server> servers, final PrimeConnectionListener listener) {
        if (servers == null) {
            return Collections.emptyList();
        }
        List<Server> allServers = new ArrayList<>();
        allServers.addAll(servers);
        if (allServers.size() == 0){
            logger.debug("RestClient:" + name + ". No nodes/servers to prime connections");
            return Collections.emptyList();
        }
        logger.info("Priming Connections for RestClient:" + name + ", numServers:" + allServers.size());
        List<Future<Boolean>> ftList = new ArrayList<Future<Boolean>>();
        for (Server s : allServers) {
            s.setReadyToServe(false);
            // 异步
            if (aSync) {
                try {
                    ftList.add(makeConnectionASync(s, listener));
                } catch (RejectedExecutionException ree) {
                    logger.error("executor submit failed", ree);
                } catch (Exception e) {
                    logger.error("general error", e);
                }
            } else {
                // 同步
                connectToServer(s, listener);
            }
        }
        return ftList;
    }

    /**
     * 异步执行prime connection
     */
    private Future<Boolean> makeConnectionASync(final Server server, final PrimeConnectionListener listener)
            throws InterruptedException, RejectedExecutionException {
        Callable<Boolean> ftConn = new Callable<Boolean>() {
            public Boolean call() throws Exception {
                logger.debug("calling primeconnections ...");
                return connectToServer(server, listener);
            }
        };
        // 提交线程池处理器
        return executorService.submit(ftConn);
    }

    /**
     * 关闭线程池
     */
    public void shutdown() {
        executorService.shutdown();
        Monitors.unregisterObject(name + "_PrimeConnection", this);
    }


    /**
     * 真正执行prime
     */
    private Boolean connectToServer(final Server server, final PrimeConnectionListener listener) {
        int tryNum = 0;
        Exception lastException;
        // prime+1
        totalCounter.increment();
        boolean success = false;
        do {
            try {
                logger.debug("Executing PrimeConnections request to server {} with path {}, tryNum={}", server, primeConnectionsURIPath, tryNum);
                success = connector.connect(server, primeConnectionsURIPath);
                successCounter.increment();
                lastException = null;
                break;
            } catch (Exception e) {
                logger.debug("Error connecting to server: {}", e.getMessage());
                lastException = e;
                sleepBeforeRetry(tryNum);
            } 
            logger.debug("server:{}, result={}, tryNum={}, maxRetries={}", server, success, tryNum, maxRetries);
            tryNum++;
        } while (!success && (tryNum <= maxRetries)); // 最大重试次数
        // set the alive flag so that it can be used by load balancers
        if (listener != null) {
            try {
                listener.primeCompleted(server, lastException);
            } catch (Exception e) {
                logger.error("Error calling PrimeComplete listener for server '{}'", server, e);
            }
        }
        logger.debug("Either done, or quitting server:{}, result={}, tryNum={}, maxRetries={}", 
        	server, success, tryNum, maxRetries);
        return success;
    }

    /**
     * 每次prime重试间隔
     */
    private void sleepBeforeRetry(int tryNum) {
        try {
            int sleep = (tryNum + 1) * 100;
            logger.debug("Sleeping for " + sleep + "ms ...");
            Thread.sleep(sleep);
        } catch (InterruptedException ignore) {
        }
    }

    /**
     * 线程工厂
     */
    static class ASyncPrimeConnectionsThreadFactory implements ThreadFactory {
        private static final AtomicInteger groupNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ASyncPrimeConnectionsThreadFactory(String name) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup(); // NOPMD
            namePrefix = "ASyncPrimeConnectionsThreadFactory-" + name + "-" + groupNumber.getAndIncrement() + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (!t.isDaemon()){
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY){
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
