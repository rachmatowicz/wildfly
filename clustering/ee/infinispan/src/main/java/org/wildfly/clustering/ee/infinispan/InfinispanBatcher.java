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

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.wildfly.clustering.ee.BatchContext;
import org.wildfly.clustering.ee.Batcher;

/**
 * A {@link Batcher} implementation based on Infinispan's {@link org.infinispan.batch.BatchContainer}, except that its transaction reference
 * is stored within the returned Batch object instead of a ThreadLocal.  This also allows the user to call {@link Batch#close()} from a
 * different thread than the one that created the {@link Batch}.  In this case, however, the user must first resume the batch
 * via {@link #resumeBatch(TransactionBatch)}.
 * @author Paul Ferraro
 */
public class InfinispanBatcher implements Batcher<TransactionBatch> {

    private static final BatchContext PASSIVE_BATCH_CONTEXT = () -> {
        // Do nothing
    };

    private static final TransactionBatch NON_TX_BATCH = new TransactionBatch() {
        @Override
        public void close() {
            // No-op
        }

        @Override
        public void discard() {
            // No-op
        }

        @Override
        public Transaction getTransaction() {
            return null;
        }

        @Override
        public TransactionBatch interpose() {
            return this;
        }
    };

    // Used to coalesce interposed transactions
    static final ThreadLocal<TransactionBatch> CURRENT_BATCH = new ThreadLocal<>();

    private static final Synchronization CURRENT_BATCH_REMOVER = new Synchronization() {
        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            CURRENT_BATCH.remove();
        }
    };

    private final TransactionManager tm;

    public InfinispanBatcher(Cache<?, ?> cache) {
        this(cache.getAdvancedCache().getTransactionManager());
    }

    public InfinispanBatcher(TransactionManager tm) {
        this.tm = tm;
    }

    @Override
    public TransactionBatch createBatch() {
        System.out.println(this.getClass().getName() + ": entering createBatch()");
        // we need a TM to suspend/resume
        if (this.tm == null) return NON_TX_BATCH;
        // is there a batch on this *thread*?
        // NB - if we are in a different invocation, but the same txn, we should reuse the Batch associated with
        // the current txn and not the current thread
        try {
            System.out.println(this.getClass().getName() + ": current txn manager: " + tm.toString());
            System.out.println(this.getClass().getName() + ": current txn is: " + tm.getTransaction());
        }
        catch(SystemException ex) {
            System.out.println("Caught system exception when getting txn!");
        }

        TransactionBatch batch = CURRENT_BATCH.get();
        if (batch != null) {
            System.out.println(this.getClass().getName() + ": current batch exists: interposing");
            // reuse the same batch - i.e. coalesce the operations in the same batch/txn
            return batch.interpose();
        }
        try {
            System.out.println(this.getClass().getName() + ": no current batch exists: creating new batch");
            // suspend the transaction associated with the current thread - shouldn't we record it?
            // Transaction suspended = this.tm.suspend()
            this.tm.suspend();
            // create a new batch
            this.tm.begin();
            Transaction tx = this.tm.getTransaction();
            // remove the current batch when the txn commits or rolls back
            // which depends on the relative number of calls to createBatch() and to close()
            tx.registerSynchronization(CURRENT_BATCH_REMOVER);
            // this represents the newly created txn on the current thread
            batch = new InfinispanBatch(tx);
            CURRENT_BATCH.set(batch);
            return batch;
        } catch (RollbackException | SystemException | NotSupportedException e) {
            throw new CacheException(e);
        }
    }

    // when the Batch closes with no prior interposing, it will commit
    // when the batch closes with prior interposing (a 3x), it will have to have more calls to close (3x)
    // interposing only applies to nested invocations to DistributableCache *on the same thread*

    @Override
    public BatchContext resumeBatch(TransactionBatch batch) {
        System.out.println(this.getClass().getName() + ": entering resumeBatch()");
        // is there an existing batch on the current thread? Called previously.
        TransactionBatch existingBatch = CURRENT_BATCH.get();
        // there is, but it is the same batch as the one we are asking to resume
        // Trivial case - nothing to suspend/resume
        if (batch == existingBatch) {
            System.out.println(this.getClass().getName() + ": existing batch same as resumed");
            // passive BatchContext does nothing upon BatchContext.close()
            return PASSIVE_BATCH_CONTEXT;
        }
        // there is not, or there is but it has no associated txn
        // this can happen when a batch is created
        Transaction tx = (batch != null) ? batch.getTransaction() : null;
        // Non-tx case, just swap thread local
        // in other words, there is no previous batch or there is a batch but no txn associated
        if ((batch == null) || (tx == null)) {
            CURRENT_BATCH.set(batch);
            // this is what is done when BatchContext goes out of scope
            // return to the original batch (wierd - for the scope of the BatchContext only?)
            return () -> {
                CURRENT_BATCH.set(existingBatch);
            };
        }
        try {
            System.out.println(this.getClass().getName() + ": existing batch not same as resumed - suspending current ");
            // there is an existing batch on the same thread (e.g. we are in the second/third call of a nested call sequence)
            // suspend that call sequence, resume the specified batch txn and set the current batch to the resumed batch
            // thus, the new txn will be used until the BatchContext goes out of scope, upon which time the existing batch
            // will be resumed
            if (existingBatch != null) {
                Transaction existingTx = this.tm.suspend();
                if (existingBatch.getTransaction() != existingTx) {
                    throw new IllegalStateException();
                }
            }
            this.tm.resume(tx);
            CURRENT_BATCH.set(batch);
            // this is what is done when BatchContext goes out of scope
            // NOTE: this is done after operation for both release() and discard()
            // suspend the resumed batch and resume the original existing batch (wierd)
            return () -> {
                try {
                    this.tm.suspend();
                    if (existingBatch != null) {
                        try {
                            this.tm.resume(existingBatch.getTransaction());
                            CURRENT_BATCH.set(existingBatch);
                        } catch (InvalidTransactionException e) {
                            throw new CacheException(e);
                        }
                    } else {
                        CURRENT_BATCH.remove();
                    }
                } catch (SystemException e) {
                    throw new CacheException(e);
                }
            };
        } catch (SystemException | InvalidTransactionException e) {
            throw new CacheException(e);
        }
    }

    // NOTE: the thread "stays the same" in that batch operations are thread-specific
    // what changes is the transaction which collects/records work done on the thread
    // so when we get a session on one thread, we can call discard on another thread and the
    // work done in the discard will be recorded in the original txn
    // after that operation is completed, we return to the (original) transaction

    @Override
    public TransactionBatch suspendBatch() {
        System.out.println(this.getClass().getName() + ": entering suspendBatch()");
        if (this.tm == null) return null;
        TransactionBatch batch = CURRENT_BATCH.get();
        if (batch != null) {
            System.out.println(this.getClass().getName() + ": current batch exists - suspending");
            try {
                Transaction tx = this.tm.suspend();
                if (batch.getTransaction() != tx) {
                    // yes, they should be the same txn
                    throw new IllegalStateException();
                }
            } catch (SystemException e) {
                throw new CacheException(e);
            } finally {
                CURRENT_BATCH.remove();
            }
        }
        return batch;
    }
}
