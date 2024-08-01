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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The counterpart to the {@link
 * org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService}. Composed with different
 * {@link LeaderRetrievalDriver}, we could retrieve the leader information from different storage.
 * The leader address as well as the current leader session ID will be retrieved from {@link
 * LeaderRetrievalDriver}.
 */
public class DefaultLeaderRetrievalService
        implements LeaderRetrievalService, LeaderRetrievalEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderRetrievalService.class);

    private final Object lock = new Object();

    private final LeaderRetrievalDriverFactory leaderRetrievalDriverFactory;

    @GuardedBy("lock")
    @Nullable
    private String lastLeaderAddress;

    @GuardedBy("lock")
    @Nullable
    private UUID lastLeaderSessionID;

    @GuardedBy("lock")
    private volatile boolean running;

    /** Listener which will be notified about leader changes. */
    private volatile LeaderRetrievalListener leaderListener;

    private LeaderRetrievalDriver leaderRetrievalDriver;

    /**
     * Creates a default leader retrieval service with specified {@link
     * LeaderRetrievalDriverFactory}.
     *
     * @param leaderRetrievalDriverFactory {@link LeaderRetrievalDriverFactory} used for creating
     *     {@link LeaderRetrievalDriver}.
     */
    public DefaultLeaderRetrievalService(
            LeaderRetrievalDriverFactory leaderRetrievalDriverFactory) {
        this.leaderRetrievalDriverFactory = checkNotNull(leaderRetrievalDriverFactory);

        this.lastLeaderAddress = null;
        this.lastLeaderSessionID = null;

        this.leaderRetrievalDriver = null;

        running = false;
    }

    @Override
    public void start(LeaderRetrievalListener listener) throws Exception {
        checkNotNull(listener, "Listener must not be null.");
        Preconditions.checkState(
                leaderListener == null,
                "DefaultLeaderRetrievalService can " + "only be started once.");

        synchronized (lock) {
            //TODO 初始化Leader监听器，传入不同的子类,在notifyLeaderAddress()被回调时，调用listener的方法;
            leaderListener = listener;

            //TODO ZooKeeperLeaderRetrievalDriverFactory创建ZooKeeperLeaderRetrievalDriver，一切需要进行注册,从zk中获取一些信息的,都被封装成了一个LeaderRetrievalDriver
            // 将自身(其是LeaderRetrievalEventHandler的实现)注入driver,最后driver会回调到该类的notifyLeaderAddress()
            leaderRetrievalDriver =
                    leaderRetrievalDriverFactory.createLeaderRetrievalDriver(
                            this, new LeaderRetrievalFatalErrorHandler());
            LOG.info("Starting DefaultLeaderRetrievalService with {}.", leaderRetrievalDriver);

            running = true;
        }
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderRetrievalService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
        }

        leaderRetrievalDriver.close();
    }

    /**
     * Called by specific {@link LeaderRetrievalDriver} to notify leader address.
     *
     * @param leaderInformation new notified leader information address. The exception will be
     *     handled by leader listener.
     */
    @Override
    @GuardedBy("lock")
    public void notifyLeaderAddress(LeaderInformation leaderInformation) {
        final UUID newLeaderSessionID = leaderInformation.getLeaderSessionID();
        final String newLeaderAddress = leaderInformation.getLeaderAddress();
        synchronized (lock) {
            if (running) {
                if (!Objects.equals(newLeaderAddress, lastLeaderAddress)
                        || !Objects.equals(newLeaderSessionID, lastLeaderSessionID)) {
                    if (LOG.isDebugEnabled()) {
                        if (newLeaderAddress == null && newLeaderSessionID == null) {
                            LOG.debug(
                                    "Leader information was lost: The listener will be notified accordingly.");
                        } else {
                            LOG.debug(
                                    "New leader information: Leader={}, session ID={}.",
                                    newLeaderAddress,
                                    newLeaderSessionID);
                        }
                    }

                    lastLeaderAddress = newLeaderAddress;
                    lastLeaderSessionID = newLeaderSessionID;

                    // Notify the listener only when the leader is truly changed.
                    // TODO 调用传入的不同listener的方法;
                    //  1.可以是JobMaster.ResourceManagerLeaderListener
                    //  2.可以是TaskExecutor.ResourceManagerLeaderListener
                    leaderListener.notifyLeaderAddress(newLeaderAddress, newLeaderSessionID);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring notification since the {} has already been closed.",
                            leaderRetrievalDriver);
                }
            }
        }
    }

    private class LeaderRetrievalFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Ignoring error notification since the service has been stopped.");
                    }
                    return;
                }

                if (throwable instanceof LeaderRetrievalException) {
                    leaderListener.handleError((LeaderRetrievalException) throwable);
                } else {
                    leaderListener.handleError(new LeaderRetrievalException(throwable));
                }
            }
        }
    }
}
