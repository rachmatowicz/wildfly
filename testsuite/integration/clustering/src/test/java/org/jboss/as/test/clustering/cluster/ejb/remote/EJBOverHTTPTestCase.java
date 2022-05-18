/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2022, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.as.test.clustering.cluster.ejb.remote;

import java.util.Collections;
import java.util.Properties;
import java.util.PropertyPermission;
import javax.naming.Context;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.arquillian.api.ServerSetup;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.test.clustering.cluster.AbstractClusteringTestCase;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.Incrementor;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.IncrementorBean;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.Result;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.StatefulIncrementorBean;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.StatelessIncrementorBean;
import org.jboss.as.test.clustering.ejb.EJBDirectory;
import org.jboss.as.test.clustering.ejb.RemoteEJBDirectory;
import org.jboss.as.test.integration.security.common.Utils;
import org.jboss.as.test.shared.CLIServerSetupTask;
import org.jboss.as.test.shared.ServerReload;
import org.jboss.as.test.shared.TestSuiteEnvironment;
import org.jboss.as.test.shared.integration.ejb.security.PermissionUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.ejb.client.Affinity;
import org.jboss.ejb.client.EJBClient;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
// import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.wildfly.common.function.ExceptionSupplier;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.COMPOSITE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.STEPS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;

/**
 * A test case for the key features of EJB/HTTP when used in conjunction with a load balancer,
 * namely:
 *   - stickiness for stateful session beans
 *   - load balancing for stateless session beans
 *
 * The test servers are configured as follows:
 *   - load balancer (load-balancer-1) @ localhost:8580
 *   - backend server (node-1) @ localhost:8080
 *   - backend server (node-2) @ localhost:8180
 *
 * @author Richard Achmatowicz
 */
@RunAsClient
@RunWith(Arquillian.class)
@ServerSetup(EJBOverHTTPTestCase.ServerSetupTask.class)
public class EJBOverHTTPTestCase extends AbstractClusteringTestCase {

    private static final int COUNT = 4;
    private static final String MODULE_NAME = EJBOverHTTPTestCase.class.getSimpleName();
    private static final String DEPLOYMENT_NAME = MODULE_NAME + ".jar";
    public static final long STATUS_REFRESH_TIMEOUT = 30_000;
    public static final int LB_OFFSET = 500;

    private static final PathAddress UNDERTOW = PathAddress.pathAddress().append(SUBSYSTEM,"undertow");
    private static final PathAddress REQUEST_LOGGING_FILTER = UNDERTOW.append("configuration","filter").append("custom-filter","request-logging-filter");
    private static final PathAddress REQUEST_LOGGING_FILETR_REF = UNDERTOW.append("server","default-server").append("host","default-host").append("filter-ref","request-logging-filter");
    private static final PathAddress DEFAULT_HTTP_LISTENER = UNDERTOW.append("server","default-server").append("http-listener","default");
    private static final PathAddress LOAD_BALANCER = UNDERTOW.append("configuration","filter").append("mod-cluster","load-balancer");

    @Deployment(name = DEPLOYMENT_1, managed = false, testable = false)
    @TargetsContainer(NODE_1)
    public static Archive<?> createDeploymentForContainer1() {
        return createDeployment();
    }

    @Deployment(name = DEPLOYMENT_2, managed = false, testable = false)
    @TargetsContainer(NODE_2)
    public static Archive<?> createDeploymentForContainer2() {
        return createDeployment();
    }

    private static Archive<?> createDeployment() {
        return ShrinkWrap.create(JavaArchive.class, DEPLOYMENT_NAME)
                .addPackage(EJBDirectory.class.getPackage())
                .addClasses(Result.class, Incrementor.class, IncrementorBean.class, StatefulIncrementorBean.class, StatelessIncrementorBean.class)
                .addAsManifestResource(PermissionUtils.createPermissionsXmlAsset(new PropertyPermission(NODE_NAME_PROPERTY, "read")), "permissions.xml")
                ;
    }

    public EJBOverHTTPTestCase() {
        super(new String[] { NODE_1, NODE_2, LOAD_BALANCER_1 }, new String[]{DEPLOYMENT_1, DEPLOYMENT_2});
    }

    @Before
    public void beforeTest() throws Exception {
        log.infof(MODULE_NAME+ " : running before test ");
        installRequestDumperIntoLoadBalancer();
    }

    @After
    public void afterTest() throws Exception {
        log.infof(MODULE_NAME+ " : running after test ");
        removeRequestDumperFromLoadBalancer();
    }

    /*
     * Run a test where the client communicates with a load balancer via JNDI/HTTP and EJB/HTTP.
     */
    @Test
    public void testEJBClientUsingHTTPProtocol() throws Exception {
        log.infof(MODULE_NAME+ " : testing without failover with client using HTTP");
        waitForProxyRegistration();

        // only http works at the moment (not https)
        testSLSBWithoutFailover(() -> new RemoteEJBDirectory(MODULE_NAME, getProperties(false)));

        // only http works at the moment (not https)
        testSFSBWithoutFailover(() -> new RemoteEJBDirectory(MODULE_NAME, getProperties(false)));

    }

    /*
     * A test which checks SLSB behaviour of EJB client with EJB/HTTP in the absence of failover.
     * The key behaviour to validate: load balancing.
     * Proxies are obtained via JNDI/HTTP and invocations are made using EJB/HTTP.
     */
    public void testSLSBWithoutFailover(ExceptionSupplier<EJBDirectory, Exception> directoryProvider) throws Exception {

        try (EJBDirectory directory = directoryProvider.get()) {
            Incrementor bean = directory.lookupStateless(StatelessIncrementorBean.class, Incrementor.class);

            Affinity strongAffinity = EJBClient.getStrongAffinity(bean);
            Affinity weakAffinity = EJBClient.getWeakAffinity(bean);
            log.info("Calling testSLSBWithoutFailover: strong affinity = " + strongAffinity + ", weak affinity = " + weakAffinity);

            Result<Integer> result = bean.increment();
            log.info("Called SLSBWithoutFailover: backend node = " + result.getNode());

            int count = 1;
            for (int i = 0; i < COUNT; ++i) {
                result = bean.increment();
                log.info("Called SLSBWithoutFailover: backend node = " + result.getNode());
            }
        }
    }

    /*
     * A test which checks SFSB behaviour of EJB client with EJB/HTTP in the absence of failover.
     * The key behaviour to validate: stickiness of EHB sessions to the nodes that own them.
     * Proxies are obtained via JNDI/HTTP and invocations are made using EJB/HTTP.
     */
    public void testSFSBWithoutFailover(ExceptionSupplier<EJBDirectory, Exception> directoryProvider) throws Exception {

        try (EJBDirectory directory = directoryProvider.get()) {
            // this single statement creates a session on the server
            Incrementor bean = directory.lookupStateful(StatefulIncrementorBean.class, Incrementor.class);

            Affinity strongAffinity = EJBClient.getStrongAffinity(bean);
            Affinity weakAffinity = EJBClient.getWeakAffinity(bean);
            log.info("Calling testSFSBWithoutFailover: strong affinity = " + strongAffinity + ", weak affinity = " + weakAffinity);

            Result<Integer> result = bean.increment();
            log.infof("Called SFSBWithoutFailover: value = %s, backend node = %s", result.getValue().intValue(), result.getNode());

            int count = 1;
            for (int i = 0; i < COUNT; ++i) {
                result = bean.increment();
                log.infof("Called SFSBWithoutFailover: value = %s, backend node = %s", result.getValue().intValue(), result.getNode());
            }
        }
    }


    /*
     * Set up JNDI properties to support HTTP based Jakarta Enterprise Beans client invocations via EJB/HTTP
     *
     * NOTE: there are several ways to connect to the Jakarta Enterprise Beans container on the server:
     *   protocol           URL
     *   remoting           remote://localhost:4447
     *   HTTP Upgrade       remote+http://localhost:8080
     *   pure HTTP          http://localhost:8080/wildfly-sevices
     */
    private static Properties getProperties(boolean isSecure) {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, org.wildfly.naming.client.WildFlyInitialContextFactory.class.getName());
        if (isSecure) {
            props.put(Context.PROVIDER_URL, String.format("%s://%s:%s/wildfly-services", "https", "localhost", "8580"));
            props.put(Context.SECURITY_PRINCIPAL, "remoteejbuser");
            props.put(Context.SECURITY_CREDENTIALS, "rem@teejbpasswd1");
        } else {
            props.put(Context.PROVIDER_URL, String.format("%s://%s:%s/wildfly-services", "http", "localhost", "8580"));
            props.put(Context.SECURITY_PRINCIPAL, "remoteejbuser");
            props.put(Context.SECURITY_CREDENTIALS, "rem@teejbpasswd1");
        }
        return props ;
    }

    private void waitForProxyRegistration() {
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            System.out.println("Thread.sleep() was interrupted");
        }
    }

    /*
     * Install a request dumper into Undertow to see which requests are arriving at the load balancer     *
     */
    private void installRequestDumperIntoLoadBalancer() throws Exception {
        final String address = TestSuiteEnvironment.getServerAddress();
        final int port = TestSuiteEnvironment.getServerPort();
        final ModelControllerClient client = TestSuiteEnvironment.getModelControllerClient(null, address, port + LB_OFFSET);

        final ModelNode compositeOp = new ModelNode();
        compositeOp.get(OP).set(COMPOSITE);
        compositeOp.get(OP_ADDR).setEmptyList();
        ModelNode steps = compositeOp.get(STEPS);

        // /subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:add(class-name=io.undertow.server.handlers.RequestDumpingHandler,module=io.undertow.core)
        ModelNode addLoggingFilterModelNode = Util.createAddOperation(REQUEST_LOGGING_FILTER);
        addLoggingFilterModelNode.get("class-name").set("io.undertow.server.handlers.RequestDumpingHandler");
        addLoggingFilterModelNode.get("module").set("io.undertow.core");
        steps.add(addLoggingFilterModelNode);

        // /subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:add()
        ModelNode addLoggingFilterRefModelNode = Util.createAddOperation(REQUEST_LOGGING_FILETR_REF);
        steps.add(addLoggingFilterRefModelNode);

        // disable http2 on the defauult connector
        ModelNode disableHttp2DefaultListenerModelNode = Util.getWriteAttributeOperation(DEFAULT_HTTP_LISTENER, "enable-http2", false);
        steps.add(disableHttp2DefaultListenerModelNode);

        // disable http2 on load-balancer
        ModelNode disableHttp2LoadBalancerModelNode = Util.getWriteAttributeOperation(LOAD_BALANCER, "enable-http2", false);
        steps.add(disableHttp2LoadBalancerModelNode);

        Utils.applyUpdates(Collections.singletonList(compositeOp), client);
        ServerReload.reloadIfRequired(client);
    }

    /*
     * Install a request dumper into Undertow to see which requests are arriving at the load balancer
     */
    private void removeRequestDumperFromLoadBalancer() throws Exception {
        final String address = TestSuiteEnvironment.getServerAddress();
        final int port = TestSuiteEnvironment.getServerPort();
        final ModelControllerClient client = TestSuiteEnvironment.getModelControllerClient(null, address, port + LB_OFFSET);

        final ModelNode compositeOp = new ModelNode();
        compositeOp.get(OP).set(COMPOSITE);
        compositeOp.get(OP_ADDR).setEmptyList();
        ModelNode steps = compositeOp.get(STEPS);

        // /subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:remove()
        ModelNode removeLoggingFilterModelNode = Util.createRemoveOperation(REQUEST_LOGGING_FILTER);
        steps.add(removeLoggingFilterModelNode);

        // /subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:remove()
        ModelNode removeLoggingFilterRefModelNode = Util.createRemoveOperation(REQUEST_LOGGING_FILETR_REF);
        steps.add(removeLoggingFilterRefModelNode);

        // enable http2 on the defauult connector
        ModelNode enableHttp2DefaultListenerModelNode = Util.getWriteAttributeOperation(DEFAULT_HTTP_LISTENER, "enable-http2", true);
        steps.add(enableHttp2DefaultListenerModelNode);

        // enable http2 on the load balancer
        ModelNode enableHttp2LoadBalancerModelNode = Util.getWriteAttributeOperation(LOAD_BALANCER, "enable-http2", true);
        steps.add(enableHttp2LoadBalancerModelNode);

        Utils.applyUpdates(Collections.singletonList(compositeOp), client);
        ServerReload.reloadIfRequired(client);
    }

    /*
     * This server setup task registers each of the servers with the load balancer.
     */
    static class ServerSetupTask extends CLIServerSetupTask {
        public ServerSetupTask() {
            this.builder
                // configure one backend server for mod_cluster registration and Undertow request dumping
               .node(NODE_1)
                // configure for mod_cluster
               .setup("/subsystem=modcluster/proxy=default:write-attribute(name=advertise,value=false)")
               .setup("/socket-binding-group=standard-sockets/remote-destination-outbound-socket-binding=proxy1:add(host=localhost,port=8590)")
               .setup("/subsystem=modcluster/proxy=default:list-add(name=proxies,value=proxy1)")
                // configure for Undertow request dumping
               .setup("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:add(class-name=io.undertow.server.handlers.RequestDumpingHandler,module=io.undertow.core")
               .setup("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:add")

               .teardown("/subsystem=modcluster/proxy=default:list-remove(name=proxies,value=proxy1)")
               .teardown("/socket-binding-group=standard-sockets/remote-destination-outbound-socket-binding=proxy1:remove()")
               .teardown("/subsystem=modcluster/proxy=default:write-attribute(name=advertise,value=true)")

               .teardown("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:remove")
               .teardown("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:remove")
               .parent()

               .node(NODE_2)
                // configure a second backend server
               .setup("/subsystem=modcluster/proxy=default:write-attribute(name=advertise,value=false)")
               .setup("/socket-binding-group=standard-sockets/remote-destination-outbound-socket-binding=proxy2:add(host=localhost,port=8590)")
               .setup("/subsystem=modcluster/proxy=default:list-add(name=proxies,value=proxy2)")
                // configure for Undertow request dumping
               .setup("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:add(class-name=io.undertow.server.handlers.RequestDumpingHandler,module=io.undertow.core")
               .setup("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:add")

               .teardown("/subsystem=modcluster/proxy=default:list-remove(name=proxies,value=proxy2)")
               .teardown("/socket-binding-group=standard-sockets/remote-destination-outbound-socket-binding=proxy2:remove()")
               .teardown("/subsystem=modcluster/proxy=default:write-attribute(name=advertise,value=true)")

               .teardown("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:remove")
               .teardown("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:remove")

                /*

                TODO: fix the server setup mechanism so we can operate on nodes which do not have deployments
               .node(LOAD_BALANCER_1)
                // configure request dumping on LB and access logging
                // request dumping
               .setup("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:add(class-name=io.undertow.server.handlers.RequestDumpingHandler,module=io.undertow.core")
               .setup("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:add")
                // access log
               .setup("/subsystem=undertow/server=default-server/host=default-host/setting=access-log:add(pattern=\"%h %t \"%r\" %s \"%{i,User-Agent}\"\", use-server-log=true)")
               .teardown("/subsystem=undertow/server=default-server/host=default-host/setting=access-log:remove")
               .teardown("/subsystem=undertow/server=default-server/host=default-host/filter-ref=request-logging-filter:remove")
               .teardown("/subsystem=undertow/configuration=filter/custom-filter=request-logging-filter:remove")

                 */
            ;
        }
    }
}
