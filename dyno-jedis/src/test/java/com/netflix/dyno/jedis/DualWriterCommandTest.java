package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.mockito.Mockito.when;

/**
 * Test generic commands with Dual Writer
 *
 * Note: The underlying jedis client has been mocked to echo back the value
 * given for SET operations
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DynoJedisPipeline.class })
public class DualWriterCommandTest {
    private DynoDualWriterClientTest dualWriterClient;

    private DynoJedisClient shadowClient;
    private ConnectionPoolImpl<Jedis> primaryconnectionPool;
    private ConnectionPoolImpl<Jedis> shadowConnectionPool;
    private OperationMonitor primaryOpMonitor;
    private OperationMonitor shadowOpMonitor;

    @Mock
    ConnectionPoolConfiguration cpConfig;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);

        when(cpConfig.isDualWriteEnabled()).thenReturn(true);
        when(cpConfig.getDualWritePercentage()).thenReturn(100);
        when(cpConfig.getHashtag()).thenReturn("{}");

        primaryOpMonitor = new LastOperationMonitor();
        shadowOpMonitor = new LastOperationMonitor();

        primaryconnectionPool = new UnitTestConnectionPool(cpConfig, primaryOpMonitor);
        shadowConnectionPool = new UnitTestConnectionPool(cpConfig, shadowOpMonitor);

        shadowClient = new DynoJedisClient.TestBuilder()
                .withAppname("DualWriterShadowClient")
                .withConnectionPool(shadowConnectionPool)
                .build();


        dualWriterClient = new DynoDualWriterClientTest.TestBuilder()
                .withAppName("DualWriterCommandTest")
                .withClusterName("DualWriterPrimaryCluster")
                .withConnectionPool(primaryconnectionPool)
                .withShadowClient(shadowClient)
                .withDualWritePercentage(100)
                .build();
    }

    @Test
    public void testDynoDualWriterGetSet() {
        String resultSet = dualWriterClient.set("Key1", "Value1");
        Assert.assertEquals("OK", resultSet);
        Assert.assertEquals(((LastOperationMonitor)primaryOpMonitor).getSuccessCount(OpName.SET.name()),
        Integer.valueOf(1));

        Assert.assertEquals(((LastOperationMonitor)primaryOpMonitor).getSuccessCount(OpName.SET.name()),
                ((LastOperationMonitor)shadowOpMonitor).getSuccessCount(OpName.SET.name()));

        // dualwriter only reads from primary client
        String resultGet = dualWriterClient.get("Key1");
        Assert.assertEquals("Value1", resultGet);
        Assert.assertEquals(((LastOperationMonitor)primaryOpMonitor).getSuccessCount(OpName.GET.name()),
                Integer.valueOf(1));

        // read from shadow client
        resultGet = shadowClient.get("Key1");
        Assert.assertEquals("Value1", resultGet);
        Assert.assertEquals(((LastOperationMonitor)shadowOpMonitor).getSuccessCount(OpName.GET.name()),
                Integer.valueOf(1));
    }

    @Test
    public void testDynoDualWriterPipelineGetSet() {
        DynoDualWriterPipeline pipeline = dualWriterClient.pipelined();
//        DynoDualWriterPipeline pipelineMock = PowerMockito.spy(pipeline);

        try {
            DynoJedisPipeline primaryPiplelineMock = PowerMockito.spy(pipeline.getPrimaryPipeline());
            DynoJedisPipeline shadowPiplelineMock = PowerMockito.spy(pipeline.getShadowPipeline());
            PowerMockito.doNothing().when(primaryPiplelineMock, "pipelined", String.class);
//            PowerMockito.doReturn(null).when(shadowPiplelineMock, "pipelined", any());
//            PowerMockito.doNothing().when(pipelineMock, "pipelined", String.class);
            PowerMockito.doNothing().when(shadowPiplelineMock, "pipelined", String.class);
        } catch (Throwable t) {
            t.printStackTrace();
        }


        Response<String> responseSet = pipeline.set("Key1", "Value1");
        pipeline.sync();

        Assert.assertEquals(((LastOperationMonitor)primaryOpMonitor).getSuccessCount(OpName.SET.name()),
                ((LastOperationMonitor)shadowOpMonitor).getSuccessCount(OpName.SET.name()));

        // pipeline only reads from primary client
        Response<String> responseGet = pipeline.get("Key1");
        Assert.assertEquals(((LastOperationMonitor)primaryOpMonitor).getSuccessCount(OpName.GET.name()),
                Integer.valueOf(1));

        // read from shadow client
        String resultGet = shadowClient.get("Key1");
        Assert.assertEquals(((LastOperationMonitor)shadowOpMonitor).getSuccessCount(OpName.GET.name()),
                Integer.valueOf(1));
    }

    /*
     * Wrapper on DynoDualWriterClient to turn
     * async writes to sync writes to shadow client.
     */
    static class DynoDualWriterClientTest extends DynoDualWriterClient {
        private final ConnectionPoolImpl<Jedis> primaryConnPool;
        private final DynoJedisClient shadowClient;

        DynoDualWriterClientTest(String name, String clusterName, ConnectionPoolImpl<Jedis> pool, DynoOPMonitor operationMonitor, ConnectionPoolMonitor connectionPoolMonitor, DynoJedisClient shadowClient, Dial dial) {
            super(name, clusterName, pool, operationMonitor, connectionPoolMonitor, shadowClient, dial);
            this.primaryConnPool = pool;
            this.shadowClient = shadowClient;
        }

        /*
         * Used for unit testing only
         */
        /* package */ static class TestBuilder {
            private String appName;
            private String clusterName;
            private DynoJedisClient sClient;
            private ConnectionPoolImpl<Jedis> connPool;
            private Dial dial;

            TestBuilder withAppName(String appName) {
                this.appName = appName;
                return this;
            }

            TestBuilder withClusterName(String clusterName) {
                this.clusterName = clusterName;
                return this;
            }

            TestBuilder withConnectionPool(ConnectionPoolImpl<Jedis> connPool) {
                this.connPool = connPool;
                return this;
            }

            TestBuilder withDualWritePercentage(int dualWritePercentage) {
                this.dial = new TimestampDial(dualWritePercentage);
                return this;
            }

            TestBuilder withShadowClient(DynoJedisClient shadowClient) {
                this.sClient = shadowClient;
                return this;
            }

            public DynoDualWriterClientTest build() {
                return new DynoDualWriterClientTest(appName, clusterName, connPool, null,
                        null, sClient, dial);
            }
        }

        @Override
        protected <R> Future<OperationResult<R>> writeAsync(final String key, Callable<OperationResult<R>> func) {
            if (sendShadowRequest(key)) {
                try {
                    func.call();
                } catch (Throwable t) {
                    // ignore
                }
            }
            return null;
        }

        /**
         *  writeAsync() for binary commands
         */
        @Override
        protected <R> Future<OperationResult<R>> writeAsync(final byte[] key, Callable<OperationResult<R>> func) {
            if (sendShadowRequest(key)) {
                try {
                    func.call();
                } catch (Throwable t) {
                    // ignore
                }
            }
            return null;
        }

        @Override
        protected boolean sendShadowRequest(String key) {
            return getDial().isInRange(key);
        }

        @Override
        protected boolean sendShadowRequest(byte[] key) {
            return getDial().isInRange(key);
        }

        @Override
        public DynoDualWriterPipeline pipelined() {
            return new DynoDualWriterPipelineTest("DynoDualWriterPipelineTest",
                    primaryConnPool,
                    new DynoJedisPipelineMonitor("DynoDualWriterPipelineTest"),
                    null,
                    shadowClient.getConnPool(),
                    getDial());
        }
    }

    static class DynoDualWriterPipelineTest extends DynoDualWriterPipeline {

        DynoDualWriterPipelineTest(String appName, ConnectionPoolImpl<Jedis> pool, DynoJedisPipelineMonitor operationMonitor, ConnectionPoolMonitor connPoolMonitor, ConnectionPoolImpl<Jedis> shadowConnectionPool, DynoDualWriterClient.Dial dial) {
            super(appName, pool, operationMonitor, connPoolMonitor, shadowConnectionPool, dial);
        }

        @Override
        <R> Future<Response<R>> writeAsync(String key, Callable<Response<R>> func) {
            if (canSendShadowRequest(key)) {
                try {
                    func.call();
                } catch (Throwable t) {
                    // ignore
                }
            }
            return null;
        }

        @Override
        <R> Future<Response<R>> writeAsync(byte[] key, Callable<Response<R>> func) {
            if (canSendShadowRequest(key)) {
                try {
                    func.call();
                } catch (Throwable t) {
                    // ignore
                }
            }
            return null;
        }

        @Override
        protected boolean canSendShadowRequest(String key) {
            return getDial().isInRange(key);
        }

        @Override
        protected boolean canSendShadowRequest(byte[] key) {
            return getDial().isInRange(key);
        }
    }
}
