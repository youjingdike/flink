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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 */
public class DefaultLeaderElectionService
        implements LeaderElectionService, LeaderElectionEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /** The leader contender which applies for leadership. */
    private volatile LeaderContender leaderContender;

    @GuardedBy("lock")
    private volatile UUID issuedLeaderSessionID;

    @GuardedBy("lock")
    private volatile UUID confirmedLeaderSessionID;

    @GuardedBy("lock")
    private volatile String confirmedLeaderAddress;

    @GuardedBy("lock")
    private volatile boolean running;

    /** ZooKeeperLeaderElectionDriver的实例 */
    private LeaderElectionDriver leaderElectionDriver;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        leaderContender = null;

        issuedLeaderSessionID = null;
        confirmedLeaderSessionID = null;
        confirmedLeaderAddress = null;

        this.leaderElectionDriver = null;

        running = false;
    }

    @Override
    public final void start(LeaderContender contender) throws Exception {
        checkNotNull(contender, "Contender must not be null.");
        Preconditions.checkState(leaderContender == null, "Contender was already set.");

        synchronized (lock) {
            /*
            TODO
             1.在WebMonitorEndpoint中调用时，此contender为MiniDispatcherRestEndpoint
             2.在ResourceManager中调用时,contender为ResourceManager
             3.在DispatcherRunner中调用时,contender为DispatcherRunner
            */
            /*
            TODO 有4中竞选者类型，LeaderContender有4种情况:
             1.Dispatcher = DefaultDispatcherRunner
             2.JobMaster = JobMasterServiceLeadershipRunner
             3.ResourceManager = ResourceManagerServiceImpl
             4.WebMonitorEndpoint = MiniDispatcherRestEndpoint
             */
            leaderContender = contender;

            // TODO 此处创建选举对象 LeaderElectionDriver，为ZooKeeperLeaderElectionDriver的实例，
            //  在创建LeaderElectionDriver时将其自身传入(其自身是LeaderElectionEventHandler的实现)
            // TODO 在创建的过程中就会启动选举，并进行监听器的回调：
            //      1.如果竞选成功，则回调该类的isLeader方法
            //      2.如果竞选失败，则回调该类的notLeader方法
            leaderElectionDriver =
                    leaderElectionDriverFactory.createLeaderElectionDriver(
                            this,
                            new LeaderElectionFatalErrorHandler(),
                            leaderContender.getDescription());
            LOG.info("Starting DefaultLeaderElectionService with {}.", leaderElectionDriver);

            running = true;
        }
    }

    @Override
    public final void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            clearConfirmedLeaderInformation();
        }

        leaderElectionDriver.close();
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);
        }

        checkNotNull(leaderSessionID);

        synchronized (lock) {
            if (hasLeadership(leaderSessionID)) {
                if (running) {
                    // TODO 确认Leader信息，并将节点信息写入zk
                    confirmLeaderInformation(leaderSessionID, leaderAddress);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Ignoring the leader session Id {} confirmation, since the "
                                        + "LeaderElectionService has already been stopped.",
                                leaderSessionID);
                    }
                }
            } else {
                // Received an old confirmation call
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Receive an old confirmation call of leader session ID {}, "
                                        + "current issued session ID is {}",
                                leaderSessionID,
                                issuedLeaderSessionID);
                    }
                } else {
                    LOG.warn(
                            "The leader session ID {} was confirmed even though the "
                                    + "corresponding JobManager was not elected as the leader.",
                            leaderSessionID);
                }
            }
        }
    }

    @Override
    public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        synchronized (lock) {
            if (running) {
                return leaderElectionDriver.hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "hasLeadership is called after the service is stopped, returning false.");
                }
                return false;
            }
        }
    }

    /**
     * Returns the current leader session ID or null, if the contender is not the leader.
     *
     * @return The last leader session ID or null, if the contender is not the leader
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID() {
        return confirmedLeaderSessionID;
    }

    @GuardedBy("lock")
    private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
        confirmedLeaderSessionID = leaderSessionID;
        confirmedLeaderAddress = leaderAddress;
        leaderElectionDriver.writeLeaderInformation(
                LeaderInformation.known(confirmedLeaderSessionID, confirmedLeaderAddress));
    }

    @GuardedBy("lock")
    private void clearConfirmedLeaderInformation() {
        confirmedLeaderSessionID = null;
        confirmedLeaderAddress = null;
    }

    @Override
    @GuardedBy("lock")
    public void onGrantLeadership() {
        synchronized (lock) {
            if (running) {
                // TODO 接受领导者的服务会将这个sessionID,通过调用该服务的confirmLeadership(..),
                //  将sessionId 和 address公布出去，供Leader检索服务使用。
                issuedLeaderSessionID = UUID.randomUUID();
                clearConfirmedLeaderInformation();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Grant leadership to contender {} with session ID {}.",
                            leaderContender.getDescription(),
                            issuedLeaderSessionID);
                }
                /*
                TODO 有4种竞选者类型，LeaderContender有4种情况:
                 1.Dispatcher = DefaultDispatcherRunner
                 2.JobMaster = JobMasterServiceLeadershipRunner
                 3.ResourceManager = ResourceManagerServiceImpl
                 4.WebMonitorEndpoint = WebMonitorEndpoint
                 */
                leaderContender.grantLeadership(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring the grant leadership notification since the {} has "
                                    + "already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    @Override
    @GuardedBy("lock")
    public void onRevokeLeadership() {
        synchronized (lock) {
            if (running) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Revoke leadership of {} ({}@{}).",
                            leaderContender.getDescription(),
                            confirmedLeaderSessionID,
                            confirmedLeaderAddress);
                }

                issuedLeaderSessionID = null;
                clearConfirmedLeaderInformation();

                leaderContender.revokeLeadership();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Clearing the leader information on {}.", leaderElectionDriver);
                }
                // Clear the old leader information on the external storage
                leaderElectionDriver.writeLeaderInformation(LeaderInformation.empty());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring the revoke leadership notification since the {} "
                                    + "has already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    @Override
    @GuardedBy("lock")
    public void onLeaderInformationChange(LeaderInformation leaderInformation) {
        // TODO 节点数据变化，会被回调到该方法;
        synchronized (lock) {
            if (running) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Leader node changed while {} is the leader with session ID {}. New leader information {}.",
                            leaderContender.getDescription(),
                            confirmedLeaderSessionID,
                            leaderInformation);
                }
                // TODO 条件：本身是已确定的Leader
                if (confirmedLeaderSessionID != null) {
                    final LeaderInformation confirmedLeaderInfo =
                            LeaderInformation.known(
                                    confirmedLeaderSessionID, confirmedLeaderAddress);
                    // TODO 变化后的数据为空,将自身信息写入
                    if (leaderInformation.isEmpty()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Writing leader information by {} since the external storage is empty.",
                                    leaderContender.getDescription());
                        }
                        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                        // TODO 返回的信息与期望的领导信息不对应,重新写入自身信息
                    } else if (!leaderInformation.equals(confirmedLeaderInfo)) {
                        // the data field does not correspond to the expected leader information
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Correcting leader information by {}.",
                                    leaderContender.getDescription());
                        }
                        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring change notification since the {} has "
                                    + "already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

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

                if (throwable instanceof LeaderElectionException) {
                    leaderContender.handleError((LeaderElectionException) throwable);
                } else {
                    leaderContender.handleError(new LeaderElectionException(throwable));
                }
            }
        }
    }
}
