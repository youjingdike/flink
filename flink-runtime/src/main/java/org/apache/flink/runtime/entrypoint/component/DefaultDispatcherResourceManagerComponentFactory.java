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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobGraphStoreFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory
        implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull private final RestEndpointFactory<?> restEndpointFactory;

    public DefaultDispatcherResourceManagerComponentFactory(
            @Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
            @Nonnull ResourceManagerFactory<?> resourceManagerFactory,
            @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    @Override
    public DispatcherResourceManagerComponent create(
            Configuration configuration,
            Executor ioExecutor,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            MetricRegistry metricRegistry,
            ExecutionGraphInfoStore executionGraphInfoStore,
            MetricQueryServiceRetriever metricQueryServiceRetriever,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        LeaderRetrievalService resourceManagerRetrievalService = null;
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        ResourceManagerService resourceManagerService = null;
        DispatcherRunner dispatcherRunner = null;

        try {
            //step1:首先初始化了一些监控服务：
            // TODO 非HA为：StandaloneLeaderRetrievalService,HA为：DefaultLeaderRetrievalService 监控 Dispatcher
            dispatcherLeaderRetrievalService =
                    highAvailabilityServices.getDispatcherLeaderRetriever();

            // TODO 类型同上， 监控 ResourceManager
            resourceManagerRetrievalService =
                    highAvailabilityServices.getResourceManagerLeaderRetriever();

            // TODO Dispatcher 的 GatewayRetriever
            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            DispatcherGateway.class,
                            DispatcherId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            // TODO ResourceManager 的 GatewayRetriever
            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            ResourceManagerGateway.class,
                            ResourceManagerId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            //step2: TODO 创建线程池，用于执行WebMonitorEndpoint所接收到的client发送过来的请求
            final ScheduledExecutorService executor =
                    WebMonitorEndpoint.createExecutorService(
                            configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
                            configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
                            "DispatcherRestEndpoint");
            //step3: TODO 初始化 MetricFetcher， 间隔是10s
            final long updateInterval =
                    configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
            final MetricFetcher metricFetcher =
                    updateInterval == 0
                            ? VoidMetricFetcher.INSTANCE
                            : MetricFetcherImpl.fromConfiguration(
                                    configuration,
                                    metricQueryServiceRetriever,
                                    dispatcherGatewayRetriever,
                                    executor);
            /*step4:
             TODO 创建WebMonitorEndpoint实例，在Standalong模式下为：DispatcherRestEndpoint
              在perjob模式下为：MiniDispatcherRestEndpoint
              该实例内部会启动一个Netty服务端，绑定了一堆Handler
            */
            webMonitorEndpoint =
                    restEndpointFactory.createRestEndpoint(
                            configuration,
                            dispatcherGatewayRetriever,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            executor,
                            metricFetcher,
                            highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
                            fatalErrorHandler);

            log.debug("Starting Dispatcher REST endpoint.");
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService);

            /*step5:
            TODO 创建ResourceManager实例
             三个要点:
             1. ResourceManager是一个RpcEndpoint(Actor),当构建好对象后启动时会触发onStart(Actor的perStart生命周期方法)方法
             2. ResourceManager也是一个LeaderContender,也会执行竞选, 会执行竞选结果方法
             3. ResourceManagerService 具有两个心跳服务和两个定时服务:
                    两个心跳服务:
                        从节点  和   主节点之间的心跳
                        Job的主控程序 和 主节点之间的心跳
                    两个定时服务:
                        TaskManager 的超时检查服务
                        Slot申请的 超时检查服务
             */
            resourceManagerService =
                    ResourceManagerServiceImpl.create(
                            resourceManagerFactory,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            fatalErrorHandler,
                            new ClusterInformation(hostname, blobServer.getPort()),
                            webMonitorEndpoint.getRestBaseUrl(),
                            metricRegistry,
                            hostname,
                            ioExecutor);

            final HistoryServerArchivist historyServerArchivist =
                    HistoryServerArchivist.createHistoryServerArchivist(
                            configuration, webMonitorEndpoint, ioExecutor);

            final PartialDispatcherServices partialDispatcherServices =
                    new PartialDispatcherServices(
                            configuration,
                            highAvailabilityServices,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            heartbeatServices,
                            () ->
                                    JobManagerMetricGroup.createJobManagerMetricGroup(
                                            metricRegistry, hostname),
                            executionGraphInfoStore,
                            fatalErrorHandler,
                            historyServerArchivist,
                            metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                            ioExecutor);

            log.debug("Starting Dispatcher.");

            /*step6:
            TODO 在该代码的内部会创建Dispatcher组件，并调用start() 方法启动
             */
            dispatcherRunner =
                    dispatcherRunnerFactory.createDispatcherRunner(
                            highAvailabilityServices.getDispatcherLeaderElectionService(),
                            fatalErrorHandler,
                            new HaServicesJobGraphStoreFactory(highAvailabilityServices),
                            ioExecutor,
                            rpcService,
                            partialDispatcherServices);

            log.debug("Starting ResourceManagerService.");
            resourceManagerService.start();

            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            return new DispatcherResourceManagerComponent(
                    dispatcherRunner,
                    resourceManagerService,
                    dispatcherLeaderRetrievalService,
                    resourceManagerRetrievalService,
                    webMonitorEndpoint,
                    fatalErrorHandler);

        } catch (Exception exception) {
            // clean up all started components
            if (dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if (resourceManagerService != null) {
                terminationFutures.add(resourceManagerService.closeAsync());
            }

            if (dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture =
                    FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException(
                    "Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE),
                resourceManagerFactory,
                SessionRestEndpointFactory.INSTANCE);
    }

    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory, JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                //TODO 第二个工厂:DefaultDispatcherRunnerFactory 内部引用 JobDispatcherLeaderProcessFactoryFactory
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
                //TODO 第一个工厂:YarnResourceManagerFactory
                resourceManagerFactory,
                //TODO 第三个工厂:JobRestEndpointFactory
                JobRestEndpointFactory.INSTANCE);
    }
}
