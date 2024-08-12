/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Runner for the {@link org.apache.flink.runtime.dispatcher.Dispatcher} which is responsible for
 * the leader election.
 */
public final class DefaultDispatcherRunner implements DispatcherRunner, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDispatcherRunner.class);

    private final Object lock = new Object();

    private final LeaderElectionService leaderElectionService;

    private final FatalErrorHandler fatalErrorHandler;

    private final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory;

    private final CompletableFuture<Void> terminationFuture;

    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private boolean running;

    private DispatcherLeaderProcess dispatcherLeaderProcess;

    private CompletableFuture<Void> previousDispatcherLeaderProcessTerminationFuture;

    private DefaultDispatcherRunner(
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler,
            DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory) {
        this.leaderElectionService = leaderElectionService;
        this.fatalErrorHandler = fatalErrorHandler;
        this.dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactory;
        this.terminationFuture = new CompletableFuture<>();
        this.shutDownFuture = new CompletableFuture<>();

        this.running = true;
        this.dispatcherLeaderProcess = StoppedDispatcherLeaderProcess.INSTANCE;
        this.previousDispatcherLeaderProcessTerminationFuture =
                CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (!running) {
                return terminationFuture;
            } else {
                running = false;
            }
        }

        stopDispatcherLeaderProcess();

        FutureUtils.forward(previousDispatcherLeaderProcessTerminationFuture, terminationFuture);

        return terminationFuture;
    }

    // ---------------------------------------------------------------
    // Leader election
    // ---------------------------------------------------------------

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        // TODO 选主之后被回调
        runActionIfRunning(
                () -> {
                    LOG.info(
                            "{} was granted leadership with leader id {}. Creating new {}.",
                            getClass().getSimpleName(),
                            leaderSessionID,
                            DispatcherLeaderProcess.class.getSimpleName());
                    // TODO
                    startNewDispatcherLeaderProcess(leaderSessionID);
                });
    }

    private void startNewDispatcherLeaderProcess(UUID leaderSessionID) {
        // 如果当前有DispatcherLeader则先关闭
        stopDispatcherLeaderProcess();

        // TODO per-job:JobDispatcherLeaderProcess
        //  application: SessionDispatcherLeaderProcess
        dispatcherLeaderProcess = createNewDispatcherLeaderProcess(leaderSessionID);

        final DispatcherLeaderProcess newDispatcherLeaderProcess = dispatcherLeaderProcess;
        // TODO 在这里:
        // TODO per-job：启动JobDispatcherLeaderProcess,调用到JobDispatcherLeaderProcess.onStart()
        // TODO
        // application：启动SessionDispatcherLeaderProcess,调用到SessionDispatcherLeaderProcess.onStart()
        FutureUtils.assertNoException(
                previousDispatcherLeaderProcessTerminationFuture.thenRun(
                        newDispatcherLeaderProcess::start));
    }

    private void stopDispatcherLeaderProcess() {
        final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();
        previousDispatcherLeaderProcessTerminationFuture =
                FutureUtils.completeAll(
                        Arrays.asList(
                                previousDispatcherLeaderProcessTerminationFuture,
                                terminationFuture));
    }

    private DispatcherLeaderProcess createNewDispatcherLeaderProcess(UUID leaderSessionID) {
        // TODO per-job：JobDispatcherLeaderProcess
        // TODO application：SessionDispatcherLeaderProcess
        final DispatcherLeaderProcess newDispatcherLeaderProcess =
                dispatcherLeaderProcessFactory.create(leaderSessionID);

        forwardShutDownFuture(newDispatcherLeaderProcess);
        // TODO 确认Leader信息;
        //  这会通知Leader选举服务，表明该竞争者已接受指定的领导权，并且现在可以公布Leader session ID和Leader地址供Leader
        // retrieval(检索)服务使用。
        forwardConfirmLeaderSessionFuture(leaderSessionID, newDispatcherLeaderProcess);

        return newDispatcherLeaderProcess;
    }

    private void forwardShutDownFuture(DispatcherLeaderProcess newDispatcherLeaderProcess) {
        newDispatcherLeaderProcess
                .getShutDownFuture()
                .whenComplete(
                        (applicationStatus, throwable) -> {
                            synchronized (lock) {
                                // ignore if no longer running or if leader processes is no longer
                                // valid
                                if (running
                                        && this.dispatcherLeaderProcess
                                                == newDispatcherLeaderProcess) {
                                    if (throwable != null) {
                                        shutDownFuture.completeExceptionally(throwable);
                                    } else {
                                        shutDownFuture.complete(applicationStatus);
                                    }
                                }
                            }
                        });
    }

    private void forwardConfirmLeaderSessionFuture(
            UUID leaderSessionID, DispatcherLeaderProcess newDispatcherLeaderProcess) {
        FutureUtils.assertNoException(
                newDispatcherLeaderProcess
                        .getLeaderAddressFuture()
                        .thenAccept(
                                leaderAddress -> {
                                    if (leaderElectionService.hasLeadership(leaderSessionID)) {
                                        leaderElectionService.confirmLeadership(
                                                leaderSessionID, leaderAddress);
                                    }
                                }));
    }

    @Override
    public void revokeLeadership() {
        runActionIfRunning(
                () -> {
                    LOG.info(
                            "{} was revoked the leadership with leader id {}. Stopping the {}.",
                            getClass().getSimpleName(),
                            dispatcherLeaderProcess.getLeaderSessionId(),
                            DispatcherLeaderProcess.class.getSimpleName());
                    this.stopDispatcherLeaderProcess();
                });
    }

    private void runActionIfRunning(Runnable runnable) {
        synchronized (lock) {
            if (running) {
                runnable.run();
            } else {
                LOG.debug(
                        "Ignoring action because {} has already been stopped.",
                        getClass().getSimpleName());
            }
        }
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new FlinkException(
                        String.format(
                                "Exception during leader election of %s occurred.",
                                getClass().getSimpleName()),
                        exception));
    }

    public static DispatcherRunner create(
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler,
            DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory)
            throws Exception {
        // TODO 创建DefaultDispatcherRunner
        final DefaultDispatcherRunner dispatcherRunner =
                new DefaultDispatcherRunner(
                        leaderElectionService, fatalErrorHandler, dispatcherLeaderProcessFactory);
        // TODO DefaultDispatcherRunner进行选主启动
        return DispatcherRunnerLeaderElectionLifecycleManager.createFor(
                dispatcherRunner, leaderElectionService);
    }
}
