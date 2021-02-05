/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.early;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.util.MultiOperationRequest;
import java.util.Queue;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.hibrid.late.HibridLockFreeNode;
import parallelism.late.graph.Vertex;
import parallelism.scheduler.Scheduler;

/**
 *
 * @author eduardo
 */
public class HibridScheduler implements Scheduler {

    private HibridClassToThreads[] classes;
    private Queue<TOMMessage>[] queues;

    public HibridScheduler(int numberOfPartitions, HibridClassToThreads[] cToT, int queuesCapacity) {
        queues = new Queue[numberOfPartitions];
        for (int i = 0; i < queues.length; i++) {
            queues[i] = new SPSCQueue(queuesCapacity);
        }
        this.classes = cToT;

        for (int i = 0; i < this.classes.length; i++) {
            Queue<TOMMessage>[] q = new Queue[this.classes[i].tIds.length];
            for (int j = 0; j < q.length; j++) {
                q[j] = queues[this.classes[i].tIds[j]];
            }
            this.classes[i].setQueues(q);
        }
    }

    public Queue<TOMMessage>[] getAllQueues() {
        return this.queues;
    }

    @Override
    public int getNumWorkers() {
        return this.queues.length;
    }

    @Override
    public ParallelMapping getMapping() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleReplicaReconfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schedule(TOMMessage request) {
        HibridClassToThreads ct = this.getClass(request.groupId);
        if (ct == null) {
            System.err.println("CLASStoTHREADs MAPPING NOT FOUND");
        } else if (ct.type == HibridClassToThreads.CONC) {//conc (só tem uma thread, então vai ser sempre na posição 0)
            boolean inserted = false;
            while (!inserted) {
                inserted = ct.queues[0].offer(request);
            }
        } else { //sync (adicionar em todas as filas)... ja cria o node FAZER O BATCH EM UM UNICO NODE AQUI

            MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
            MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
            for (int i = 0; i < reqs.operations.length; i++) {
                TOMMessageWrapper mw = new TOMMessageWrapper(new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId, ctx));
                mw.msg.node = new HibridLockFreeNode(mw.msg, Vertex.MESSAGE, null, queues.length, ct.tIds.length);
                mw.msg.threadId = ct.tIds[ct.threadIndex];
                ct.threadIndex = (ct.threadIndex + 1) % ct.tIds.length;

                for (Queue q : ct.queues) {
                    boolean inserted = false;
                    while (!inserted) {
                        inserted = q.offer(mw);
                    }
                }
            }
        }
    }

    @Override
    public void schedule(MessageContextPair request) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

        /*HibridClassToThreads ct = this.getClass(request.classId);
        if (ct == null) {
            System.err.println("CLASStoTHREADs MAPPING NOT FOUND");
        }else if (ct.type == HibridClassToThreads.CONC) {//conc (só tem uma thread, então vai ser sempre na posição 0)
            boolean inserted = false;
            while (!inserted) {
                inserted = ct.queues[0].offer(request);
            }
        } else { //sync (adicionar em todas as filas)... ja cria o node
            request.node = new HibridLockFreeNode(request, Vertex.MESSAGE, null, queues.length, ct.tIds.length);
            request.threadId = ct.tIds[ct.threadIndex];
            ct.threadIndex = (ct.threadIndex+1)%ct.tIds.length;
            
            for (Queue q : ct.queues) {
                boolean inserted = false;
                while (!inserted) {
                    inserted = q.offer(request);
                }
            }
        }*/
    }

    public HibridClassToThreads getClass(int id) {
        for (int i = 0; i < this.classes.length; i++) {
            if (this.classes[i].classId == id) {
                return this.classes[i];
            }
        }
        return null;
    }

    public int getExecutorThread(int classId) {
        return this.getClass(classId).tIds[0];
    }
}
