package com.netflix.hystrix;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class HystrixCircuitBreakerTest2 {
    private static final Integer timeout = 25;
    private static final String KEY_PREFIX = "CircuitBreakerTestKey";

    @Before
    public void init() {
        for (HystrixCommandMetrics metricsInstance: HystrixCommandMetrics.getInstances()) {
            metricsInstance.resetStream();
        }

        HystrixCommandMetrics.reset();
        HystrixCircuitBreaker.Factory.reset();
        Hystrix.reset();
    }

    @Test
    public void testRaceConditionCauseOpenForever() {
        AtomicBoolean running = new AtomicBoolean(true);
        // make Thread Busy
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long i = 0;
                    while (running.get()) {
                        i++;
                    }
                }
            });
            thread.setPriority(Thread.MAX_PRIORITY);
            thread.start();
        }

        try {
            for (int i = 0; i < 10000; i++) {
                String key = KEY_PREFIX + "" + i;
                HystrixCommand<String> cmd1 = new HystrixCommand4CircuitBreaker(key, 0, true);
                HystrixCircuitBreaker.HystrixCircuitBreakerImpl cb = (HystrixCircuitBreaker.HystrixCircuitBreakerImpl) HystrixCircuitBreaker.Factory.getInstance(HystrixCommandKey.Factory.asKey(key));
                while (!cb.isOpen()) {
                    HystrixCommand<String> failCommand = new HystrixCommand4CircuitBreaker(key, 0, true);
                    failCommand.execute();
                }

                do {
                    Thread.sleep(1);
                } while (!cb.allowRequest());
                System.out.println("==================================try testRaceConditionCauseOpenForever#"+i+"==============================");

                System.out.println("[before]cb allowRequest:" + cb.allowRequest() + " cb isOpen:" + cb.isOpen());

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        String name = "successTryCommand";
                        HystrixCommand<String> command = new HystrixCommand4CircuitBreaker(key, timeout - 2, false);
                        command.execute();
                        Throwable executionException = command.getExecutionException();
                        if (executionException == null) {
                            System.out.println("[" + name + "]success");
                        } else {
                            System.out.println("[" + name + "]fallback cause:" + executionException.getClass().getSimpleName() + " message:" + executionException.getMessage());
                        }
                    }
                }).start();

                Thread.sleep(timeout - 1);
                if (!cb.isOpen()) {
                    continue;
                }

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        String name = "fallbackCommand";
                        HystrixCommand<String> command = new HystrixCommand4CircuitBreaker(key, 0, true);
                        command.execute();
                        Throwable executionException = command.getExecutionException();
                        if (executionException == null) {
                            System.out.println("[" + name + "]success");
                        } else {
                            System.out.println("[" + name + "]fallback cause:" + executionException.getClass().getSimpleName() + " message:" + executionException.getMessage());
                        }
                    }
                }).start();

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        String name = "failureTryCommand";
                        HystrixCommand<String> command = new HystrixCommand4CircuitBreaker(key, timeout, false);
                        command.execute();
                        Throwable executionException = command.getExecutionException();
                        if (executionException == null) {
                            System.out.println("[" + name + "]success");
                        } else {
                            System.out.println("[" + name + "]fallback cause:" + executionException.getClass().getSimpleName() + " message:" + executionException.getMessage());
                        }
                    }
                }).start();

                Thread.sleep(timeout + 50);

                HystrixCircuitBreaker.HystrixCircuitBreakerImpl.Status status = cb.getStatus();
                long circuitOpened = cb.getCircuitOpened().get();
                if (circuitOpened > 0 && status == HystrixCircuitBreaker.HystrixCircuitBreakerImpl.Status.CLOSED) {
                    running.set(false);

                    System.out.println("==========================reproduce==========================");
                    System.out.println("circuit breaker isOpen:" + cb.isOpen());
                    System.out.println("circuit breaker isAfterSleepWindow:" + cb.isAfterSleepWindow());
                    System.out.println("circuit breaker allowRequest:" + cb.allowRequest());
                    System.out.println("==========================reproduce==========================");

                    HystrixCommand<String> successCommand = new HystrixCommand4CircuitBreaker(key, 0, false);
                    String execute = successCommand.execute();
                    Throwable executionException = successCommand.getExecutionException();

                    assertNull("command should success but fail result: " + execute + "fallback cause:" + executionException.getClass().getSimpleName() + " message:" + executionException.getMessage(),executionException);
                    break;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    private static class HystrixCommand4CircuitBreaker extends HystrixCommand<String> {
        private final Integer sleepTime;
        private final boolean fail;

        public HystrixCommand4CircuitBreaker(String key, Integer sleepTime, boolean fail) {
            super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("CircuitBreakerTestGroup"))
                    .andCommandKey(HystrixCommandKey.Factory.asKey(key))
                    .andCommandPropertiesDefaults(
                            HystrixCommandProperties.Setter()
                                    .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                                    .withExecutionIsolationSemaphoreMaxConcurrentRequests(100)
                                    .withCircuitBreakerRequestVolumeThreshold(4)
                                    .withExecutionTimeoutInMilliseconds(timeout)
                                    .withCircuitBreakerSleepWindowInMilliseconds(timeout)
                                    .withCircuitBreakerErrorThresholdPercentage(50)
                    )
            );
            this.sleepTime = sleepTime;
            this.fail = fail;
        }

        @Override
        protected String run() throws Exception {
            Thread.sleep(sleepTime);
            if (fail) {
                throw new RuntimeException();
            }
            return "success";
        }

        @Override
        protected String getFallback() {
            return "fallback";
        }
    }
}