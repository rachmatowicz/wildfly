/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
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

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.as.test.clustering.EJBClientContextSelector;
import org.jboss.as.test.clustering.cluster.ClusterAbstractTestCase;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.Incrementor;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.Result;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.SecureStatelessIncrementorBean;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.SlowToDestroyStatefulIncrementorBean;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.StatefulIncrementorBean;
import org.jboss.as.test.clustering.cluster.ejb.remote.bean.StatelessIncrementorBean;
import org.jboss.as.test.clustering.ejb.EJBDirectory;
import org.jboss.as.test.clustering.ejb.RemoteEJBDirectory;
import org.jboss.as.test.shared.TimeoutUtil;
import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.logging.Logger;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A small-scale stress test for @Stateful vs @Stateless behavior of a remotely accessed clustered session beans.
 *
 * @author Richard Achmatowicz
 */
@RunWith(Arquillian.class)
@RunAsClient
public class RemoteStressTestCase extends ClusterAbstractTestCase {
    private static final Logger log = Logger.getLogger(RemoteStressTestCase.class);
    private static final String MODULE_NAME = "remote-failover-test";
    private static final String CLIENT_PROPERTIES = "org/jboss/as/test/clustering/cluster/ejb/remote/jboss-ejb-client.properties";
    private static final String SECURE_CLIENT_PROPERTIES = "org/jboss/as/test/clustering/cluster/ejb/remote/jboss-ejb-client-secure.properties";

    private static final int ITERATION_COUNT = 5;
    private static final long ITERATION_WAIT = TimeoutUtil.adjust(1000 * 60);
    private static final int THREADS_PER_ITERATION = 5;
    private static final long CLIENT_TOPOLOGY_UPDATE_WAIT = TimeoutUtil.adjust(5000);
    private static final long INVOCATION_WAIT = TimeoutUtil.adjust(100);

    @Deployment(name = DEPLOYMENT_1, managed = false, testable = false)
    @TargetsContainer(CONTAINER_1)
    public static Archive<?> createDeploymentForContainer1() {
        return createDeployment();
    }

    @Deployment(name = DEPLOYMENT_2, managed = false, testable = false)
    @TargetsContainer(CONTAINER_2)
    public static Archive<?> createDeploymentForContainer2() {
        return createDeployment();
    }

    private static Archive<?> createDeployment() {
        final JavaArchive jar = ShrinkWrap.create(JavaArchive.class, MODULE_NAME + ".jar");
        jar.addPackage(Incrementor.class.getPackage());
        jar.addPackage(EJBDirectory.class.getPackage());
        log.info(jar.toString(true));
        return jar;
    }

    public void testStatefulInvocationsUnderStress() throws Exception {
        testInvocationsUnderStress("STATELESS", ITERATION_COUNT, THREADS_PER_ITERATION, ITERATION_WAIT, INVOCATION_WAIT);
    }

    @Test
    public void testStatelessInvocationsUnderStress() throws Exception {
        testInvocationsUnderStress("STATEFUL", ITERATION_COUNT, THREADS_PER_ITERATION, ITERATION_WAIT, INVOCATION_WAIT);
    }

    private void testInvocationsUnderStress(String beanType, int num_iterations, int num_new_threads, long time_between_iterations, long time_between_invocations) throws Exception {
        // run the stress test

        int total_thread_count = num_iterations * num_new_threads;

        InvokerTask[] invokers = new InvokerTask[total_thread_count];

        for (int i = 0; i < num_iterations; i++) {

            System.out.println("*** Starting iteration " + i + " ***") ;
            System.out.println("*** Total thread count " + num_new_threads * (i+1)) ;

            // create threads for this iteration
            for (int j = 0; j < num_new_threads; j++) {
                int thread_id = i*num_new_threads + j ;
                if (beanType == "STATEFUL")
                    invokers[thread_id] = new StatefulInvokerTask(thread_id, time_between_invocations);
                else if (beanType == "STATELESS")
                    invokers[thread_id] = new StatefulInvokerTask(thread_id, time_between_invocations);
                else
                    invokers[thread_id] = new InvokerTask(thread_id, time_between_invocations);
            }

            // start threads for this iteration
            for (int j = 0; j < num_new_threads; j++) {
                int thread_id = i* num_new_threads + j ;
                invokers[thread_id].start();
            }

            // wait for iteration to complete
            try {
                Thread.sleep(time_between_iterations) ;
            }
            catch(InterruptedException ie) {
                System.out.println("Task was interrupted");
            }
        }

        // stop the threads and wait to terminate
        for (int j = 0; j < total_thread_count; j++) {
            invokers[j].stopRunning();
            invokers[j].join();
        }
        System.out.println("Terminated");

    }


    private class InvokerTask extends Thread {
        protected int i;
        protected long time_between_invocations;
        protected boolean running = false;

        public InvokerTask(int i, long time_between_invocations) {
            this.i = i;
            this.time_between_invocations = time_between_invocations;
        }

        public int getIndex() {
            return i;
        }

        public void run() {
            running = true;
            while (running == true) {
                System.out.println("Invoker task " + i + " is sleeping");
                try {
                    Thread.sleep(time_between_invocations);
                } catch (InterruptedException ie) {
                    System.out.println("Task was interrupted");
                }
            }
        }

        public void stopRunning() {
            running = false;
        }
    }

    private class StatefulInvokerTask extends InvokerTask {

        public StatefulInvokerTask(int i, long time_between_invocations) {
            super(i, time_between_invocations);
        }

        @Override
        public void run() {
            // establish the EJBClient context
            ContextSelector<EJBClientContext> selector = null;
            try {
                selector = EJBClientContextSelector.setup(CLIENT_PROPERTIES);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            try (EJBDirectory context = new RemoteEJBDirectory(MODULE_NAME)) {
                // get the bean we will invoke on
                Incrementor bean = context.lookupStateful(StatefulIncrementorBean.class, Incrementor.class);

                while (running == true) {
                    // make an invocation
                    Result<Integer> result = bean.increment();

                    // wait a while before making the next invocation
                    try {
                        Thread.sleep(time_between_invocations);
                    } catch (InterruptedException ie) {
                        System.out.println("Task was interrupted");
                    }
                }

            } catch (Exception ex) {
                ex.printStackTrace();

            } finally {
                EJBClientContext.setSelector(selector);
            }
        }
    }

    private class StatelessInvokerTask extends InvokerTask {

        public StatelessInvokerTask(int i, long time_between_invocations) {
            super(i, time_between_invocations);
        }

        @Override
        public void run() {
            // establish the EJBClient context
            ContextSelector<EJBClientContext> selector = null;
            try {
                selector = EJBClientContextSelector.setup(CLIENT_PROPERTIES);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            try (EJBDirectory context = new RemoteEJBDirectory(MODULE_NAME)) {
                // get the bean we will invoke on
                Incrementor bean = context.lookupStateful(StatelessIncrementorBean.class, Incrementor.class);

                while (running == true) {
                    // make an invocation
                    Result<Integer> result = bean.increment();

                    // wait a while before making the next invocation
                    try {
                        Thread.sleep(time_between_invocations);
                    } catch (InterruptedException ie) {
                        System.out.println("Task was interrupted");
                    }
                }

            } catch (Exception ex) {
                ex.printStackTrace();

            } finally {
                EJBClientContext.setSelector(selector);
            }
        }
    }

}
