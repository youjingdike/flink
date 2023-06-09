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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility class implements the basis of registering one component at another component, for
 * example registering the TaskExecutor at the ResourceManager. This {@code RetryingRegistration}
 * implements both the initial address resolution and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * registration can be canceled, for example when the target where it tries to register at looses
 * leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RetryingRegistration<
        F extends Serializable,
        G extends RpcGateway,
        S extends RegistrationResponse.Success,
        R extends RegistrationResponse.Rejection> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final Logger log;

    private final RpcService rpcService;

    private final String targetName;

    private final Class<G> targetType;

    private final String targetAddress;

    private final F fencingToken;

    private final CompletableFuture<RetryingRegistrationResult<G, S, R>> completionFuture;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private volatile boolean canceled;

    // ------------------------------------------------------------------------

    public RetryingRegistration(
            Logger log,
            RpcService rpcService,
            String targetName,
            Class<G> targetType,
            String targetAddress,
            F fencingToken,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.log = checkNotNull(log);
        this.rpcService = checkNotNull(rpcService);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.completionFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  completion and cancellation
    // ------------------------------------------------------------------------

    public CompletableFuture<RetryingRegistrationResult<G, S, R>> getFuture() {
        return completionFuture;
    }

    /** Cancels the registration procedure. */
    public void cancel() {
        canceled = true;
        completionFuture.cancel(false);
    }

    /**
     * Checks if the registration was canceled.
     *
     * @return True if the registration was canceled, false otherwise.
     */
    public boolean isCanceled() {
        return canceled;
    }

    // ------------------------------------------------------------------------
    //  registration
    // ------------------------------------------------------------------------

    protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
            G gateway, F fencingToken, long timeoutMillis) throws Exception;

    /**
     * This method resolves the target address to a callable gateway and starts the registration
     * after that.
     */
    @SuppressWarnings("unchecked")
    public void startRegistration() {
        if (canceled) {
            // we already got canceled
            return;
        }

        try {
            // trigger resolution of the target address to a callable gateway
            final CompletableFuture<G> rpcGatewayFuture;

            // TODO 这里的RPCGateway相当于主节点的一个引用(ActorRef),后续的注册使用的是这个引用
            if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
                rpcGatewayFuture =
                        (CompletableFuture<G>)
                                rpcService.connect(
                                        targetAddress,
                                        fencingToken,
                                        targetType.asSubclass(FencedRpcGateway.class));
            } else {
                // TODO 可以是TaskExecutor连接ResourceManager
                rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
            }

            // upon success, start the registration attempts
            // TODO 如果连接建立成功,获取到了RPCGateWay,接下来会有三次回调
            CompletableFuture<Void> rpcGatewayAcceptFuture =

                    // TODO 异步注册
                    rpcGatewayFuture.thenAcceptAsync(
                            (G rpcGateway) -> {
                                log.info("Resolved {} address, beginning registration", targetName);
                                // TODO 使用这个RpcGateway引用对象进行注册
                                register(
                                        rpcGateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis());
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry, unless this is cancelled
            // TODO 异步注册的回调
            rpcGatewayAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        // TODO 如果失败,且并非手动取消
                        if (failure != null && !canceled) {
                            final Throwable strippedFailure =
                                    ExceptionUtils.stripCompletionException(failure);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Could not resolve {} address {}, retrying in {} ms.",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure);
                            } else {
                                log.info(
                                        "Could not resolve {} address {}, retrying in {} ms: {}",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure.getMessage());
                            }
                            // TODO 如果注册失败,尝试再次注册,延时调度,时长通过cluster.registration.error-delay参数进行配置,默认10s
                            startRegistrationLater(
                                    retryingRegistrationConfiguration.getErrorDelayMillis());
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    /**
     * This method performs a registration attempt and triggers either a success notification or a
     * retry, depending on the result.
     */
    @SuppressWarnings("unchecked")
    private void register(final G gateway, final int attempt, final long timeoutMillis) {
        // eager check for canceling to avoid some unnecessary work
        if (canceled) {
            return;
        }

        try {
            log.debug(
                    "Registration at {} attempt {} (timeout={}ms)",
                    targetName,
                    attempt,
                    timeoutMillis);
            // TODO 真正开始注册
            // TODO 调用子类的实现方法
            CompletableFuture<RegistrationResponse> registrationFuture =
                    invokeRegistration(gateway, fencingToken, timeoutMillis);

            // if the registration was successful, let the TaskExecutor know
            // TODO registrationFuture成功获取到返回结果，没有报错
            CompletableFuture<Void> registrationAcceptFuture =
                    registrationFuture.thenAcceptAsync(
                            (RegistrationResponse result) -> {
                                if (!isCanceled()) {
                                    // TODO 注册成功
                                    if (result instanceof RegistrationResponse.Success) {
                                        log.debug(
                                                "Registration with {} at {} was successful.",
                                                targetName,
                                                targetAddress);
                                        S success = (S) result;
                                        // TODO 此方法执行完将触发回调:onRegistrationSuccess方法
                                        // 回调方法触发的地方在我们之前构建TaskExecutor注册对象的方法createNewRegistration()中
                                        completionFuture.complete(
                                                RetryingRegistrationResult.success(
                                                        gateway, success));
                                    // TODO 拒绝注册
                                    } else if (result instanceof RegistrationResponse.Rejection) {
                                        log.debug(
                                                "Registration with {} at {} was rejected.",
                                                targetName,
                                                targetAddress);
                                        R rejection = (R) result;
                                        // TODO 此方法执行完将触发回调:onRegistrationFailure方法
                                        //回调方法触发的地方在我们之前构建TaskExecutor注册对象的方法createNewRegistration()中
                                        completionFuture.complete(
                                                RetryingRegistrationResult.rejection(rejection));
                                    } else {
                                        // registration failure
                                        // TODO 其他原因注册失败
                                        if (result instanceof RegistrationResponse.Failure) {
                                            RegistrationResponse.Failure failure =
                                                    (RegistrationResponse.Failure) result;
                                            log.info(
                                                    "Registration failure at {} occurred.",
                                                    targetName,
                                                    failure.getReason());
                                        } else {
                                            log.error(
                                                    "Received unknown response to registration attempt: {}",
                                                    result);
                                        }

                                        log.info(
                                                "Pausing and re-attempting registration in {} ms",
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());

                                        // TODO 重试
                                        registerLater(
                                                gateway,
                                                1,
                                                retryingRegistrationConfiguration
                                                        .getInitialRegistrationTimeoutMillis(),
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                    }
                                }
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry
            // TODO 如果注册失败
            registrationAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !isCanceled()) {
                            // TODO 如果因为超时
                            if (ExceptionUtils.stripCompletionException(failure)
                                    instanceof TimeoutException) {
                                // we simply have not received a response in time. maybe the timeout
                                // was
                                // very low (initial fast registration attempts), maybe the target
                                // endpoint is
                                // currently down.
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Registration at {} ({}) attempt {} timed out after {} ms",
                                            targetName,
                                            targetAddress,
                                            attempt,
                                            timeoutMillis);
                                }
                                // TODO 每超时一次,超时时间*2,
                                //  但是不能超过retryingRegistrationConfiguration.getMaxRegistrationTimeoutMillis()
                                long newTimeoutMillis =
                                        Math.min(
                                                2 * timeoutMillis,
                                                retryingRegistrationConfiguration
                                                        .getMaxRegistrationTimeoutMillis());
                                // TODO 重试次数+1
                                register(gateway, attempt + 1, newTimeoutMillis);
                            } else {
                                // a serious failure occurred. we still should not give up, but keep
                                // trying
                                log.error(
                                        "Registration at {} failed due to an error",
                                        targetName,
                                        failure);
                                log.info(
                                        "Pausing and re-attempting registration in {} ms",
                                        retryingRegistrationConfiguration.getErrorDelayMillis());

                                registerLater(
                                        gateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis(),
                                        retryingRegistrationConfiguration.getErrorDelayMillis());
                            }
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    private void registerLater(
            final G gateway, final int attempt, final long timeoutMillis, long delay) {
        rpcService.scheduleRunnable(
                new Runnable() {
                    @Override
                    public void run() {
                        register(gateway, attempt, timeoutMillis);
                    }
                },
                delay,
                TimeUnit.MILLISECONDS);
    }

    private void startRegistrationLater(final long delay) {
        rpcService.scheduleRunnable(this::startRegistration, delay, TimeUnit.MILLISECONDS);
    }

    static final class RetryingRegistrationResult<G, S, R> {
        @Nullable private final G gateway;

        @Nullable private final S success;

        @Nullable private final R rejection;

        private RetryingRegistrationResult(
                @Nullable G gateway, @Nullable S success, @Nullable R rejection) {
            this.gateway = gateway;
            this.success = success;
            this.rejection = rejection;
        }

        boolean isSuccess() {
            return success != null && gateway != null;
        }

        boolean isRejection() {
            return rejection != null;
        }

        public G getGateway() {
            Preconditions.checkState(isSuccess());
            return gateway;
        }

        public R getRejection() {
            Preconditions.checkState(isRejection());
            return rejection;
        }

        public S getSuccess() {
            Preconditions.checkState(isSuccess());
            return success;
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> success(G gateway, S success) {
            return new RetryingRegistrationResult<>(gateway, success, null);
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> rejection(R rejection) {
            return new RetryingRegistrationResult<>(null, null, rejection);
        }
    }
}
