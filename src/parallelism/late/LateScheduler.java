/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.late;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.util.MultiOperationRequest;
import parallelism.late.graph.COS;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.late.graph.CoarseGrainedLock;
import parallelism.late.graph.FineGrainedLock;
import parallelism.late.graph.LockFreeGraph;
import parallelism.scheduler.Scheduler;

/**
 *
 * @author eduardo
 */
public class LateScheduler implements Scheduler{

    private COS cos;
    private int numWorkers;
    
     //private ConflictDefinition conflictDef;
    
    public LateScheduler(int numWorkers, COSType cosType) {
        this(null, numWorkers, cosType);
    }

    public LateScheduler(ConflictDefinition cd, int numWorkers, COSType cosType) {
        //cos = new COS(150,graphType,this);
        int limit = 150;

        if(cd == null){
            cd = new DefaultConflictDefinition();
        }
        
        if(cosType == null || cosType == COSType.coarseLockGraph){
            this.cos = new CoarseGrainedLock(limit, cd);
        }else if(cosType == COSType.fineLockGraph){
            this.cos = new FineGrainedLock(limit, cd);
        }else if (cosType == COSType.lockFreeGraph){
            this.cos = new LockFreeGraph(limit, cd);
        }else{
           this.cos = new CoarseGrainedLock(limit, cd);
        }
        this.numWorkers = numWorkers;        
    }

    
    /*public boolean isDependent(MessageContextPair thisRequest, MessageContextPair otherRequest){
        if(thisRequest.classId == ParallelMapping.CONFLICT_RECONFIGURATION || 
                otherRequest.classId == ParallelMapping.CONFLICT_RECONFIGURATION){
            return true;
        }
        return this.conflictDef.isDependent(thisRequest, otherRequest);
    }*/
    
    
    @Override
    public int getNumWorkers() {
        return this.numWorkers;
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
        try {
            cos.insert(request);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        
    }

    public Object get(){
        
        try {
            return cos.get();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    public void remove(Object requestRequest){
        try {
            cos.remove(requestRequest);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
    
    @Override
    public void scheduleReplicaReconfiguration() {
        MessageContextPair m = 
                new MessageContextPair(null, ParallelMapping.CONFLICT_RECONFIGURATION, -1, (short) 0, (short) ParallelMapping.CONFLICT_RECONFIGURATION,null);
        schedule(m);
    }

    @Override
    public ParallelMapping getMapping() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
}
