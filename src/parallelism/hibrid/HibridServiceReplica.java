/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid;

import parallelism.hibrid.early.EarlySchedulerMapping;
import parallelism.hibrid.late.HibridCOS;
import parallelism.hibrid.late.MultiPartitionCOS;
import parallelism.hibrid.late.vNode;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.util.MultiOperationRequest;
import bftsmart.util.ThroughputStatistics;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.ClassToThreads;
import parallelism.ExecutionFIFOQueue;
import parallelism.FIFOQueue;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.ParallelServiceReplica;
import parallelism.hibrid.early.HibridClassToThreads;
import parallelism.hibrid.early.HibridScheduler;
import parallelism.hibrid.early.TOMMessageWrapper;
import parallelism.hibrid.late.ExtendedLockFreeGraph;
import parallelism.hibrid.late.HibridLockFreeNode;
import parallelism.late.ConflictDefinition;
import parallelism.late.graph.COS;
import parallelism.late.graph.Vertex;
import parallelism.scheduler.DefaultScheduler;
import parallelism.scheduler.Scheduler;

/**
 *
 * @author eduardo
 */
public class HibridServiceReplica extends ParallelServiceReplica {

    private ExtendedLockFreeGraph[] subgraphs;

    //public ThroughputStatistics earlyStatistics;
    public HibridServiceReplica(int id, Executable executor, Recoverable recoverer, int numPartitions, ConflictDefinition cd, int lateWorkers) {
        super(id, executor, recoverer, numPartitions);
        System.out.println("Criou um hibrid scheduler: partitions (early) = " + numPartitions + " workers (late) = " + lateWorkers);
        //this.cos = new HibridCOS(150, cd, earlyWorkers, this.scheduler.getMapping());

        String path = "resultsHibrid_" + id + "_" + numPartitions + "_" + lateWorkers + ".txt";
        statistics = new ThroughputStatistics(id, lateWorkers, path, "");

        this.subgraphs = new ExtendedLockFreeGraph[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            this.subgraphs[i] = new ExtendedLockFreeGraph(cd, i, 150 / numPartitions);
        }

        initLateWorkers(lateWorkers, id, numPartitions);

    }

    @Override
    protected void createScheduler(int initialWorkers) {
        if (initialWorkers <= 0) {
            initialWorkers = 1;

        }

        this.scheduler = new HibridScheduler(initialWorkers,
                new EarlySchedulerMapping().generateMappings(initialWorkers), 100000000);
    }

    @Override
    protected void initWorkers(int n, int id) {

        System.out.println("n early: " + n);
        int tid = 0;
        for (int i = 0; i < n; i++) {
            new EarlyWorker(tid, ((HibridScheduler) this.scheduler).getAllQueues()[i]).start();
            tid++;
        }

    }

    protected void initLateWorkers(int n, int id, int partitions) {

        System.out.println("n late: " + n);
        int tid = 0;
        for (int i = 0; i < n; i++) {

            new LateWorker(tid, partitions).start();
            tid++;
        }
    }

    private class EarlyWorker extends Thread {

        private int thread_id;
        private Queue<TOMMessage> reqs = null;

        public EarlyWorker(int id, Queue<TOMMessage> reqs) {
            this.thread_id = id;
            this.reqs = reqs;
        }

        @Override
        public void run() {
            while (true) {
                //MessageContextPair msg = reqs.poll();
                TOMMessage request = reqs.poll();
                if (request != null) {
                    HibridClassToThreads ct = ((HibridScheduler) scheduler).getClass(request.getGroupId());
                    if (ct.type == HibridClassToThreads.CONC) {
                        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                        MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
                        for (int i = 0; i < reqs.operations.length; i++) {

                            subgraphs[thread_id].insert(new HibridLockFreeNode(
                                    new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId, ctx),
                                    Vertex.MESSAGE, subgraphs[thread_id], subgraphs.length, 0), false, false);

                        }

                    } else if (ct.type == HibridClassToThreads.SYNC) {
                        TOMMessageWrapper mw = (TOMMessageWrapper) request;

                        if (mw.msg.threadId == thread_id) {
                            mw.msg.node.graph = subgraphs[thread_id];
                            subgraphs[thread_id].insert(mw.msg.node, false, true);
                        } else {
                            subgraphs[thread_id].insert(mw.msg.node, true, true);
                        }
                    }
                }
            }
        }
    }

    private class LateWorker extends Thread {

        private int thread_id;
        private int myPartition = 0;

        public LateWorker(int id, int partitions) {
            this.thread_id = id;
            this.myPartition = this.thread_id % partitions;
        }

        public byte[] serialize(short opId, short value) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream oos = new DataOutputStream(baos);

                oos.writeShort(opId);

                oos.writeShort(value);

                oos.close();
                return baos.toByteArray();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        public void run() {
            //System.out.println("rum: " + thread_id);
            MessageContextPair msg = null;
            //int exec = 0;
            while (true) {
                try {
                    //get
                    HibridLockFreeNode node = subgraphs[this.myPartition].get();
                    //execute
                    msg = node.getAsRequest();
                    //msg.resp = ((SingleExecutable) executor).executeOrdered(msg.operation, null);
                    msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.operation), null);

                    //MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                    //if (msg.ctx != null) {
                    msg.ctx.add(msg.index, msg.resp);
                    if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                        msg.ctx.finished = true;
                        msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                                msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());
                        replier.manageReply(msg.ctx.request, null);
                    }
                    //}
                    statistics.computeStatistics(thread_id, 1);
                    //remove
                    subgraphs[this.myPartition].remove(node);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

    }
}
