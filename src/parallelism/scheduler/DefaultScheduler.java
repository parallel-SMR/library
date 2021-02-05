/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.scheduler;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.util.MultiOperationRequest;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.ClassToThreads;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;

/**
 *
 * @author eduardo
 */
public class DefaultScheduler implements Scheduler {

    protected ParallelMapping mapping;

    public DefaultScheduler(int numberWorkers, ClassToThreads[] cToT) {
        this.mapping = new ParallelMapping(numberWorkers, cToT);
    }

    @Override
    public int getNumWorkers() {
        return this.mapping.getNumWorkers();
    }

    @Override
    public ParallelMapping getMapping() {
        return mapping;
    }

    @Override
    public void scheduleReplicaReconfiguration() {
        TOMMessage reconf = new TOMMessage(0, 0, 0, 0, null, 0, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONFLICT_RECONFIGURATION);
        MessageContextPair m
                = new MessageContextPair(reconf, ParallelMapping.CONFLICT_RECONFIGURATION, -1, (short) 0, ParallelMapping.CONFLICT_RECONFIGURATION, null);

        /*BlockingQueue[] q = this.getMapping().getAllQueues();
        try {
            for (BlockingQueue q1 : q) {
                q1.put(m);
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }*/
        Queue[] q = this.getMapping().getAllQueues();
        for (Queue q1 : q) {
            boolean inserted = false;
            while (!inserted) {
                inserted = q1.offer(m);
            }
        }

    }

    @Override
    public void schedule(TOMMessage request) {
        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
        MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
        for (int i = 0; i < reqs.operations.length; i++) {
            this.schedule(new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId, ctx));
        }
    }

    @Override
    public void schedule(MessageContextPair request) {
        // try {

        ClassToThreads ct = this.mapping.getClass(request.classId);
        if (ct == null) {
            //TRATAR COMO CONFLICT ALL
            //criar uma classe que sincroniza tudo
            System.err.println("CLASStoTHREADs MAPPING NOT FOUND");
           
        }

        if (ct.type == ClassToThreads.CONC) {//conc
            boolean inserted = false;
            while (!inserted) {
                inserted = ct.queues[ct.executorIndex].offer(request);
            }
            //ct.queues[ct.executorIndex].put(request);
            ct.executorIndex = (ct.executorIndex + 1) % ct.queues.length;

        } else { //sync
            for (Queue q : ct.queues) {
                boolean inserted = false;
                while (!inserted) {
                    inserted = q.offer(request);
                }
                //q.put(request);
            }
        }
        /* } catch (InterruptedException ex) {
            ex.printStackTrace();
            Logger.getLogger(DefaultScheduler.class.getName()).log(Level.SEVERE, null, ex);
        }*/
    }

    /*public void schedule(Object request, int classId) {
        try {
            
            ClassToThreads ct = this.mapping.getClass(classId);
            if(ct == null){
                //TRATAR COMO CONFLICT ALL
                //criar uma classe que sincroniza tudo
                System.err.println("CLASStoTHREADs MAPPING NOT FOUND");
            }
            
            if(ct.type == ClassToThreads.CONC){//conc
                ct.queues[ct.executorIndex].put(request);
                ct.executorIndex = (ct.executorIndex+1)% ct.queues.length;
                
            }else{ //sync
                for (BlockingQueue q : ct.queues) {
                    q.put(request);
                }
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            Logger.getLogger(DefaultScheduler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }*/
}
