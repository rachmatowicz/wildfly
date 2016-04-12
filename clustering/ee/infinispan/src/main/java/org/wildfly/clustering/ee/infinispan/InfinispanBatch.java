/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
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
package org.wildfly.clustering.ee.infinispan;

import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.commons.CacheException;

/**
 * Abstract {@link TransactionBatch} that associates and exposes the underlying transaction.
 * @author Paul Ferraro
 */
public class InfinispanBatch implements TransactionBatch {

    private final Transaction tx;
    private volatile boolean rollback = false;
    private final AtomicInteger count = new AtomicInteger(0);

    public InfinispanBatch(Transaction tx) {
        this.tx = tx;
    }

    @Override
    public Transaction getTransaction() {
        return this.tx;
    }

    @Override
    public TransactionBatch interpose() {
        this.count.incrementAndGet();
        System.out.println(this.getClass().getName() + ": interposing batch: count = " + this.count.get());
        return this;
    }

    @Override
    public void discard() {
        System.out.println(this.getClass().getName() + ": discarding batch");
        this.rollback = true;
    }

    @Override
    public void close() {
        // This is what is done when Batch goes out of scope
        System.out.println(this.getClass().getName() + ": closing batch - count = " + this.count.get());
        if (this.count.getAndDecrement() == 0) {
            try {
                if (this.rollback) {
                    System.out.println(this.getClass().getName() + ": closing batch - rollback");
                    this.tx.rollback();
                } else {
                    System.out.println(this.getClass().getName() + ": closing batch - commit");
                    this.tx.commit();
                }
            } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException e) {
                throw new CacheException(e);
            }
        }
    }
}