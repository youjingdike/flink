/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemUtils;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    public static final ConfigOption<String> INTERNAL_CLUSTER_EXECUTION_MODE =
            ConfigOptions.key("internal.cluster.execution-mode")
                    .defaultValue(ExecutionMode.NORMAL.toString());

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final Configuration configuration;

    private final CompletableFuture<ApplicationStatus> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @GuardedBy("lock")
    private DispatcherResourceManagerComponent clusterComponent;

    @GuardedBy("lock")
    private MetricRegistryImpl metricRegistry;

    @GuardedBy("lock")
    private ProcessMetricGroup processMetricGroup;

    @GuardedBy("lock")
    private HighAvailabilityServices haServices;

    @GuardedBy("lock")
    private BlobServer blobServer;

    @GuardedBy("lock")
    private HeartbeatServices heartbeatServices;

    @GuardedBy("lock")
    private RpcService commonRpcService;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    private ExecutionGraphInfoStore executionGraphInfoStore;

    private final Thread shutDownHook;
    private RpcSystem rpcSystem;

    protected ClusterEntrypoint(Configuration configuration) {
        this.configuration = generateClusterConfiguration(configuration);
        this.terminationFuture = new CompletableFuture<>();

        if (configuration.get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE
                && !supportsReactiveMode()) {
            final String msg =
                    "Reactive mode is configured for an unsupported cluster type. At the moment, reactive mode is only supported by standalone application clusters (bin/standalone-job.sh).";
            // log message as well, otherwise the error is only shown in the .out file of the
            // cluster
            LOG.error(msg);
            throw new IllegalConfigurationException(msg);
        }

        shutDownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> this.closeAsync().join(), getClass().getSimpleName(), LOG);
    }

    public CompletableFuture<ApplicationStatus> getTerminationFuture() {
        return terminationFuture;
    }

    public void startCluster() throws ClusterEntrypointException {
        LOG.info("Starting {}.", getClass().getSimpleName());

        try {
            FlinkSecurityManager.setFromConfiguration(configuration);
            PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            configureFileSystems(configuration, pluginManager);

            SecurityContext securityContext = installSecurityContext(configuration);

            ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);
            securityContext.runSecured(
                    (Callable<Void>)
                            () -> {
                                runCluster(configuration, pluginManager);

                                return null;
                            });
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

            try {
                // clean up any partial state
                shutDownAsync(
                                ApplicationStatus.FAILED,
                                ShutdownBehaviour.STOP_APPLICATION,
                                ExceptionUtils.stringifyException(strippedThrowable),
                                false)
                        .get(
                                INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(),
                                TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }

            throw new ClusterEntrypointException(
                    String.format(
                            "Failed to initialize the cluster entrypoint %s.",
                            getClass().getSimpleName()),
                    strippedThrowable);
        }
    }

    protected boolean supportsReactiveMode() {
        return false;
    }

    private void configureFileSystems(Configuration configuration, PluginManager pluginManager) {
        LOG.info("Install default filesystem.");
        FileSystem.initialize(configuration, pluginManager);
    }

    private SecurityContext installSecurityContext(Configuration configuration) throws Exception {
        LOG.info("Install security context.");

        SecurityUtils.install(new SecurityConfiguration(configuration));

        return SecurityUtils.getInstalledContext();
    }

    private void runCluster(Configuration configuration, PluginManager pluginManager)
            throws Exception {
        /*关于Flink的主节点JobManager，他只是一个逻辑上的主节点，针对不同的部署模式，主节点的实现类也不同。

        JobManager（逻辑）有三大核心内容，分别为ResourceManager、Dispatcher和WebmonitorEndpoin：

        ResourceManager：

        Flink集群的资源管理器，只有一个，关于Slot的管理和申请等工作，都有它负责

        Dispatcher：

        1、负责接收用户提交的JobGraph，然后启动一个JobMaster，类似与Yarn中的AppMaster和Spark中的Driver。

        2、内有一个持久服务：JobGraphStore，负责存储JobGraph。当构建执行图或物理执行图时主节点宕机并恢复，则可以从这里重新拉取作业JobGraph

        WebMonitorEndpoint：

        Rest服务，内部有一个Netty服务，客户端的所有请求都由该组件接收处理

        用一个例子来描述这三个组件的功能：

            当Client提交一个Job到集群时（Client会把Job构建成一个JobGraph），主节点接收到提交的job的Rest请求后，WebMonitorEndpoint 会通过Router进行解析找到对应的Handler来执行处理，
            处理完毕后交由Dispatcher，Dispatcher负责大气JobMaster来负责这个Job内部的Task的部署执行，执行Task所需的资源，JobMaster向ResourceManager申请。
         */

        synchronized (lock) {
            /**
             TODO 初始化了主节点对外提供服务的时候所需要的三大核心组件启动时所需的基础服务
             1. commonRPCService：  基于Akka的RpcService实现。内部包装了ActorSystem
             2. JMXService：        启动一个JMXService
             3. ioExecutor：        启动一个线程池
             4. haServices：        提供对高可用性所需的所有服务的访问注册，分布式计数器和领导人选举
             5. blobServer：        负责侦听传入的请求生成线程来处理这些请求 。它还负责创建要存储的目录结构blob或临时缓存目录
             6. heartbeatServices： 提供心跳所需的所有服务，这包括创建心跳接收器和心跳发送者。
             7. metricRegistry：    跟踪所有已注册的Metric，他作为连接MetricGroup和MetricReporter
             8. archivedExecutionGraphStore：存储执行图ExecutionGraph的可序列化形式。
             */
            initializeServices(configuration, pluginManager);

            // write host information into configuration
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

            /*
            TODO 此处核心方法，初始化了一个DefaultDispatcherResourceManagerComponentFactory 工厂实例，
             内部初始化了三大核心组件的工厂实例，不同模式使用的组件有可能不同：
             1. Dispatcher = DefaultDispatcherRunnerFactory 内部引用 JobDispatcherLeaderProcessFactoryFactory，
                生产DispatcherRunnerLeaderElectionLifecycleManager 内部引用
                   DefaultDispatcherRunner
             2. ResourceManager = YarnResourceManagerFactory， 生产YarnResourceManagerDriver
             3. WebMonitorEndpoint = JobRestEndpointFactory，生产 MiniDispatcherRestEndpoint
             */
            final DispatcherResourceManagerComponentFactory
                    dispatcherResourceManagerComponentFactory =
                            createDispatcherResourceManagerComponentFactory(configuration);

            /*
            * TODO 根据第一步中已创建基础服务，创建JobManager的三大核心角色实例
            1. WebMonitorEndpoint：用于接受客户端发送的执行任务的Rest请求
            2. resourceManager：负责资源的分配和记账
            3. dispatcher：负责用于接收作业提交，持久化他们，生成要执行的作业管理器任务，并在主任务失败时恢复它们。
             */
            clusterComponent =
                    dispatcherResourceManagerComponentFactory.create(
                            configuration,
                            ioExecutor,
                            commonRpcService,
                            haServices,
                            blobServer,
                            heartbeatServices,
                            metricRegistry,
                            executionGraphInfoStore,
                            new RpcMetricQueryServiceRetriever(
                                    metricRegistry.getMetricQueryServiceRpcService()),
                            this);

            clusterComponent
                    .getShutDownFuture()
                    .whenComplete(
                            (ApplicationStatus applicationStatus, Throwable throwable) -> {
                                if (throwable != null) {
                                    shutDownAsync(
                                            ApplicationStatus.UNKNOWN,
                                            ShutdownBehaviour.STOP_APPLICATION,
                                            ExceptionUtils.stringifyException(throwable),
                                            false);
                                } else {
                                    // This is the general shutdown path. If a separate more
                                    // specific shutdown was
                                    // already triggered, this will do nothing
                                    shutDownAsync(
                                            applicationStatus,
                                            ShutdownBehaviour.STOP_APPLICATION,
                                            null,
                                            true);
                                }
                            });
        }
    }


    /**
    TODO 初始化了主节点对外提供服务的时候所需要的三大核心组件启动时所需的基础服务
     1. commonRPCService：  基于Akka的RpcService实现。内部包装了ActorSystem
     2. JMXService：        启动一个JMXService
     3. ioExecutor：        启动一个线程池
     4. haServices：        提供对高可用性所需的所有服务的访问注册，分布式计数器和领导人选举
     5. blobServer：        负责侦听传入的请求生成线程来处理这些请求 。它还负责创建要存储的目录结构blob或临时缓存目录
     6. heartbeatServices： 提供心跳所需的所有服务，这包括创建心跳接收器和心跳发送者。
     7. metricRegistry：    跟踪所有已注册的Metric，他作为连接MetricGroup和MetricReporter
     8. archivedExecutionGraphStore：存储执行图ExecutionGraph的可序列化形式。
     */
    protected void initializeServices(Configuration configuration, PluginManager pluginManager)
            throws Exception {

        LOG.info("Initializing cluster services.");

        synchronized (lock) {
            /**
             * rpcSystem是{@link org.apache.flink.runtime.rpc.akka.CleanupOnCloseRpcSystem}实例，
             * 实际持有的是 {@link org.apache.flink.runtime.rpc.akka.AkkaRpcSystem}的实例
             * 用于下面创建AkkaRpcService实例
             */
            rpcSystem = RpcSystem.load(configuration);
            /**RpcService：AkkaRpcService
             *初始化和启动 AkkaRpcService，内部其实包装了一个 ActorSystem
             * */
            commonRpcService =
                    RpcUtils.createRemoteRpcService(
                            rpcSystem,
                            configuration,
                            configuration.getString(JobManagerOptions.ADDRESS),
                            getRPCPortRange(configuration),
                            configuration.getString(JobManagerOptions.BIND_HOST),
                            configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));
            // 启动一个 JMXService，用于客户端链接 JobManager JVM 进行监控
            JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

            // update the configuration used to create the high availability services
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());


            // 初始化一个负责 IO 的线程池
            ioExecutor =
                    Executors.newFixedThreadPool(
                            ClusterEntrypointUtils.getPoolSize(configuration),
                            new ExecutorThreadFactory("cluster-io"));
            /**初始化 HA 服务组件，非高可用为：StandaloneHaServices，高可用为：ZooKeeperHaServices*/
            haServices = createHaServices(configuration, ioExecutor, rpcSystem);

            // 初始化 BlobServer 服务端
            //初始化大文件存储BlobServer服务端，
            // 所谓大文件例如上传Flink-job的jar时所依赖的一些需要一起上传的jar，或者TaskManager上传的log文件等
            blobServer = new BlobServer(configuration, haServices.createBlobStore());
            blobServer.start();
            // 初始化心跳服务组件, heartbeatServices = HeartbeatServices
            heartbeatServices = createHeartbeatServices(configuration);

            // 启动 metrics（性能监控） 相关的服务，内部也是启动一个 ActorSystem
            metricRegistry = createMetricRegistry(configuration, pluginManager, rpcSystem);

            final RpcService metricQueryServiceRpcService =
                    MetricUtils.startRemoteMetricsRpcService(
                            configuration, commonRpcService.getAddress(), rpcSystem);
            metricRegistry.startQueryService(metricQueryServiceRpcService, null);

            final String hostname = RpcUtils.getHostname(commonRpcService);

            processMetricGroup =
                    MetricUtils.instantiateProcessMetricGroup(
                            metricRegistry,
                            hostname,
                            ConfigurationUtils.getSystemResourceMetricsProbingInterval(
                                    configuration));

            // 初始化一个用来存储 ExecutionGraph 的 Store, 实现是：MemoryExecutionGraphInfoStore
            executionGraphInfoStore =
                    createSerializableExecutionGraphStore(
                            configuration, commonRpcService.getScheduledExecutor());
        }
    }

    /**
     * Returns the port range for the common {@link RpcService}.
     *
     * @param configuration to extract the port range from
     * @return Port range for the common {@link RpcService}
     */
    protected String getRPCPortRange(Configuration configuration) {
        if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
            return configuration.getString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
        } else {
            return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
        }
    }

    protected HighAvailabilityServices createHaServices(
            Configuration configuration, Executor executor, RpcSystemUtils rpcSystemUtils)
            throws Exception {
        return HighAvailabilityServicesUtils.createHighAvailabilityServices(
                configuration,
                executor,
                AddressResolution.NO_ADDRESS_RESOLUTION,
                rpcSystemUtils,
                this);
    }

    protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
        return HeartbeatServices.fromConfiguration(configuration);
    }

    protected MetricRegistryImpl createMetricRegistry(
            Configuration configuration,
            PluginManager pluginManager,
            RpcSystemUtils rpcSystemUtils) {
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(
                        configuration, rpcSystemUtils.getMaximumMessageSizeInBytes(configuration)),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

        return shutDownAsync(
                        ApplicationStatus.UNKNOWN,
                        ShutdownBehaviour.STOP_PROCESS,
                        "Cluster entrypoint has been closed externally.",
                        false)
                .thenAccept(ignored -> {});
    }

    protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
        final long shutdownTimeout =
                configuration.getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);

        synchronized (lock) {
            Throwable exception = null;

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (blobServer != null) {
                try {
                    blobServer.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (haServices != null) {
                try {
                    if (cleanupHaData) {
                        haServices.closeAndCleanupAllData();
                    } else {
                        haServices.close();
                    }
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (executionGraphInfoStore != null) {
                try {
                    executionGraphInfoStore.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (processMetricGroup != null) {
                processMetricGroup.close();
            }

            if (metricRegistry != null) {
                terminationFutures.add(metricRegistry.shutdown());
            }

            if (ioExecutor != null) {
                terminationFutures.add(
                        ExecutorUtils.nonBlockingShutdown(
                                shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));
            }

            if (commonRpcService != null) {
                terminationFutures.add(commonRpcService.stopService());
            }

            try {
                JMXService.stopInstance();
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    public void onFatalError(Throwable exception) {
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(exception);
        LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

        FlinkSecurityManager.forceProcessExit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------
    // Internal methods
    // --------------------------------------------------

    private Configuration generateClusterConfiguration(Configuration configuration) {
        final Configuration resultConfiguration =
                new Configuration(Preconditions.checkNotNull(configuration));

        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);
        final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

        resultConfiguration.setString(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

        return resultConfiguration;
    }

    private CompletableFuture<ApplicationStatus> shutDownAsync(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics,
            boolean cleanupHaData) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info(
                    "Shutting {} down with application status {}. Diagnostics {}.",
                    getClass().getSimpleName(),
                    applicationStatus,
                    diagnostics);

            final CompletableFuture<Void> shutDownApplicationFuture =
                    closeClusterComponent(applicationStatus, shutdownBehaviour, diagnostics);

            final CompletableFuture<Void> serviceShutdownFuture =
                    FutureUtils.composeAfterwards(
                            shutDownApplicationFuture, () -> stopClusterServices(cleanupHaData));

            final CompletableFuture<Void> rpcSystemClassLoaderCloseFuture =
                    FutureUtils.runAfterwards(serviceShutdownFuture, rpcSystem::close);

            final CompletableFuture<Void> cleanupDirectoriesFuture =
                    FutureUtils.runAfterwards(
                            rpcSystemClassLoaderCloseFuture, this::cleanupDirectories);

            cleanupDirectoriesFuture.whenComplete(
                    (Void ignored2, Throwable serviceThrowable) -> {
                        if (serviceThrowable != null) {
                            terminationFuture.completeExceptionally(serviceThrowable);
                        } else {
                            terminationFuture.complete(applicationStatus);
                        }
                    });
        }

        return terminationFuture;
    }

    /**
     * Close cluster components and deregister the Flink application from the resource management
     * system by signalling the {@link ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param shutdownBehaviour shutdown behaviour
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    private CompletableFuture<Void> closeClusterComponent(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics) {
        synchronized (lock) {
            if (clusterComponent != null) {
                switch (shutdownBehaviour) {
                    case STOP_APPLICATION:
                        return clusterComponent.stopApplication(applicationStatus, diagnostics);
                    case STOP_PROCESS:
                    default:
                        return clusterComponent.stopProcess();
                }
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
     *
     * @throws IOException if the temporary directories could not be cleaned up
     */
    protected void cleanupDirectories() throws IOException {
        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);

        FileUtils.deleteDirectory(new File(webTmpDir));
    }

    // --------------------------------------------------
    // Abstract methods
    // --------------------------------------------------

    protected abstract DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration)
                    throws IOException;

    protected abstract ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) throws IOException;

    protected static EntrypointClusterConfiguration parseArguments(String[] args)
            throws FlinkParseException {
        final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser =
                new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

        return clusterConfigurationParser.parse(args);
    }

    protected static Configuration loadConfiguration(
            EntrypointClusterConfiguration entrypointClusterConfiguration) {
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        entrypointClusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        final int restPort = entrypointClusterConfiguration.getRestPort();

        if (restPort >= 0) {
            configuration.setInteger(RestOptions.PORT, restPort);
        }

        final String hostname = entrypointClusterConfiguration.getHostname();

        if (hostname != null) {
            configuration.setString(JobManagerOptions.ADDRESS, hostname);
        }

        return configuration;
    }

    // --------------------------------------------------
    // Helper methods
    // --------------------------------------------------

    public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

        final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
        try {
            clusterEntrypoint.startCluster();
        } catch (ClusterEntrypointException e) {
            LOG.error(
                    String.format("Could not start cluster entrypoint %s.", clusterEntrypointName),
                    e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        int returnCode;
        Throwable throwable = null;

        try {
            returnCode = clusterEntrypoint.getTerminationFuture().get().processExitCode();
        } catch (Throwable e) {
            throwable = ExceptionUtils.stripExecutionException(e);
            returnCode = RUNTIME_FAILURE_RETURN_CODE;
        }

        LOG.info(
                "Terminating cluster entrypoint process {} with exit code {}.",
                clusterEntrypointName,
                returnCode,
                throwable);
        System.exit(returnCode);
    }

    /** Execution mode of the {@link MiniDispatcher}. */
    public enum ExecutionMode {
        /** Waits until the job result has been served. */
        NORMAL,

        /** Directly stops after the job has finished. */
        DETACHED
    }

    private enum ShutdownBehaviour {
        STOP_APPLICATION,
        STOP_PROCESS,
    }
}
