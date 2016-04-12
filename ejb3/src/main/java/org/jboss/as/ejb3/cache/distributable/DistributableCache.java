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
package org.jboss.as.ejb3.cache.distributable;

import org.jboss.as.ejb3.cache.Cache;
import org.jboss.as.ejb3.cache.Contextual;
import org.jboss.as.ejb3.cache.Identifiable;
import org.jboss.as.ejb3.cache.StatefulObjectFactory;
import org.jboss.ejb.client.Affinity;
import org.wildfly.clustering.ee.Batch;
import org.wildfly.clustering.ee.BatchContext;
import org.wildfly.clustering.ejb.Bean;
import org.wildfly.clustering.ejb.BeanManager;
import org.wildfly.clustering.ejb.RemoveListener;

/**
 * Distributable {@link Cache} implementation.
 * This object is responsible for:
 * <ul>
 *     <li>Group association based on creation context</li>
 *     <li>Batching of bean manager operations</li>
 * </ul>
 * @author Paul Ferraro
 *
 * @param <K> the cache key type
 * @param <V> the cache value type
 */
public class DistributableCache<K, V extends Identifiable<K> & Contextual<Batch>> implements Cache<K, V> {
    private final BeanManager<K, V, Batch> manager;
    private final StatefulObjectFactory<V> factory;
    private final RemoveListener<V> listener;

    public DistributableCache(BeanManager<K, V, Batch> manager, StatefulObjectFactory<V> factory) {
        this.manager = manager;
        this.factory = factory;
        this.listener = new RemoveListenerAdapter<>(factory);
    }

    @Override
    public Affinity getStrictAffinity() {
        System.out.println(this.getClass().getName() + ": getStrictAffinity() - start");
        try (Batch batch = this.manager.getBatcher().createBatch()) {
            Affinity affinity = this.manager.getStrictAffinity();
            System.out.println(this.getClass().getName() + ": getStrictAffinity() - end");
            return affinity;
        }
    }

    @Override
    public Affinity getWeakAffinity(K id) {
        System.out.println(this.getClass().getName() + ": getWeakAffinity() - start");
        try (Batch batch = this.manager.getBatcher().createBatch()) {
            Affinity affinity =  this.manager.getWeakAffinity(id);
            System.out.println(this.getClass().getName() + ": getWeakAffinity() - end");
            return affinity;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public K createIdentifier() {
        System.out.println(this.getClass().getName() + ": createIdentifier() - start");
        K id = this.manager.getIdentifierFactory().createIdentifier();
        K group = (K) CURRENT_GROUP.get();
        if (group == null) {
            group = id;
            CURRENT_GROUP.set(group);
        }
        System.out.println(this.getClass().getName() + ": createIdentifier() - end");
        return id;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V create() {
        System.out.println(this.getClass().getName() + ": create() - start");
        boolean newGroup = CURRENT_GROUP.get() == null;
        try (Batch batch = this.manager.getBatcher().createBatch()) {
            try {
                // This will invoke Cache.create() for nested beans
                // Nested beans will share the same group identifier
                V instance = this.factory.createInstance();
                K id = instance.getId();
                this.manager.createBean(id, (K) CURRENT_GROUP.get(), instance).close();
                return instance;
            } catch (RuntimeException | Error e) {
                batch.discard();
                throw e;
            }
        } finally {
            if (newGroup) {
                CURRENT_GROUP.remove();
            }
            System.out.println(this.getClass().getName() + ": create() - end");
        }
    }

    @Override
    public V get(K id) {
        System.out.println(this.getClass().getName() + ": get(" + id + ") - start");
        // Batch is not closed here - it will be closed during release(...) or discard(...)
        // not closed because the instance is saved in the value
        @SuppressWarnings("resource")
        Batch batch = this.manager.getBatcher().createBatch();
        try {
            Bean<K, V> bean = this.manager.findBean(id);
            if (bean == null) {
                batch.close();
                System.out.println(this.getClass().getName() + ": get() - end");
                return null;
            }
            V result = bean.acquire();
            result.setCacheContext(batch);
            System.out.println(this.getClass().getName() + ": get() - end");
            return result;
        } catch (RuntimeException | Error e) {
            batch.discard();
            batch.close();
            throw e;
        }
    }

    @Override
    public void release(V value) {
        System.out.println(this.getClass().getName() + ": release() - start");
        try (BatchContext context = this.manager.getBatcher().resumeBatch(value.getCacheContext())) {
            try (Batch batch = value.getCacheContext()) {
                try {
                    Bean<K, V> bean = this.manager.findBean(value.getId());
                    if (bean != null) {
                        if (bean.release()) {
                            bean.close();
                        }
                    }
                    System.out.println(this.getClass().getName() + ": release() - end");
                } catch (RuntimeException | Error e) {
                    batch.discard();
                    throw e;
                }
            }
        }
    }

    @Override
    public void remove(K id) {
        System.out.println(this.getClass().getName() + ": remove() - start");
        try (Batch batch = this.manager.getBatcher().createBatch()) {
            try {
                Bean<K, V> bean = this.manager.findBean(id);
                if (bean != null) {
                    bean.remove(this.listener);
                }
                System.out.println(this.getClass().getName() + ": remove() - end");
            } catch (RuntimeException | Error e) {
                batch.discard();
                throw e;
            }
        }
    }

    @Override
    public void discard(V value) {
        System.out.println(this.getClass().getName() + ": discard() - start");
        try (BatchContext context = this.manager.getBatcher().resumeBatch(value.getCacheContext())) {
            try (Batch batch = value.getCacheContext()) {
                try {
                    Bean<K, V> bean = this.manager.findBean(value.getId());
                    if (bean != null) {
                        bean.remove(null);
                    }
                    System.out.println(this.getClass().getName() + ": discard() - end");
                } catch (RuntimeException | Error e) {
                    batch.discard();
                    throw e;
                }
            }
        }
    }

    @Override
    public boolean contains(K id) {
        System.out.println(this.getClass().getName() + ": contains() - start");
        try (Batch batch = this.manager.getBatcher().createBatch()) {
            boolean result = this.manager.containsBean(id);
            System.out.println(this.getClass().getName() + ": contains() - end");
            return result ;
        }
    }

    @Override
    public void start() {
        this.manager.start();
    }

    @Override
    public void stop() {
        this.manager.stop();
    }

    @Override
    public int getCacheSize() {
        return this.manager.getActiveCount();
    }

    @Override
    public int getPassivatedCount() {
        return this.manager.getPassiveCount();
    }

    @Override
    public int getTotalSize() {
        return this.manager.getActiveCount() + this.manager.getPassiveCount();
    }

    @Override
    public boolean isRemotable(Throwable throwable) {
        return this.manager.isRemotable(throwable);
    }
}
