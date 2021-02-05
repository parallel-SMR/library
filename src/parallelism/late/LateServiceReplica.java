/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.late;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.util.ThroughputStatistics;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.concurrent.CyclicBarrier;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.ParallelServiceReplica;
import parallelism.late.graph.DependencyGraph;

/**
 *
 * @author eduardo
 */
public class LateServiceReplica extends ParallelServiceReplica {

    private CyclicBarrier recBarrier = new CyclicBarrier(2);

    public LateServiceReplica(int id, Executable executor, Recoverable recoverer, int numWorkers, ConflictDefinition cf, COSType graphType, int partitions) {
        super(id, executor, recoverer, new LateScheduler(cf, numWorkers, graphType));
        String path = "resultsLockFree_" + id + "_"+ partitions + "_"+ numWorkers + ".txt";
        statistics = new ThroughputStatistics(id, numWorkers, path, "late");
    }

    public CyclicBarrier getReconfBarrier() {
        return recBarrier;
    }

    @Override
    public int getNumActiveThreads() {
        return this.scheduler.getNumWorkers();
    }

    @Override
    protected void initWorkers(int n, int id) {

        //statistics = new ThroughputStatistics(id, n, "results_" + id + ".txt", "");

        int tid = 0;
        for (int i = 0; i < n; i++) {
            new LateServiceReplicaWorker((LateScheduler) this.scheduler, tid).start();
            tid++;
        }
    }

    private class LateServiceReplicaWorker extends Thread {

        private LateScheduler s;
        private int thread_id;

        public LateServiceReplicaWorker(LateScheduler s, int id) {
            this.thread_id = id;
            this.s = s;

            //System.out.println("Criou um thread: " + id);
        }
        //int exec = 0;
        
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
            while (true) {
                // System.out.println("vai pegar...");
                Object node = s.get();
                //System.out.println("Pegou req...");
                
                msg = ((DependencyGraph.vNode) node).getAsRequest();
                //System.out.println("Pegou req..."+ msg.toString());
                if (msg.classId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                    try {
                        getReconfBarrier().await();
                        getReconfBarrier().await();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                } else {
                    //msg.resp = ((SingleExecutable) executor).executeOrdered( msg.operation, null);
                    //exec++;
                    msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.operation), null);

                    //System.out.println(thread_id+" Executadas: "+exec);
                    
                    //MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                    msg.ctx.add(msg.index, msg.resp);
                    if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                        msg.ctx.finished = true;
                        msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                                msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());
                        //bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                        //      + msg.message.getSender());
                        replier.manageReply(msg.ctx.request, null);
                    }
                    statistics.computeStatistics(thread_id, 1);
                }
                
                //s.removeRequest(msg);
                s.remove(node);
            }
        }

    }
}
