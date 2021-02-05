package parallelism.hibrid.late;

import demo.list.MultipartitionMapping;
import parallelism.ClassToThreads;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;
import parallelism.late.ConflictDefinition;
import parallelism.late.graph.Vertex;

public class HibridCOS extends MultiPartitionCOS {

    private ExtendedLockFreeGraph[] subgraphs;
    private ParallelMapping pm;

    public HibridCOS(int limit, ConflictDefinition cd, int numSubgraphs, ParallelMapping pm) {
        super(limit, cd, numSubgraphs);
        this.subgraphs = new ExtendedLockFreeGraph[numSubgraphs];
        for (int i = 0; i < numSubgraphs; i++) {
            this.subgraphs[i] = new ExtendedLockFreeGraph(cd,i,150);
        }
        this.pm = pm;
    }

    //int callc = 0;
    
    @Override
    protected Object COSGet(int partition) throws InterruptedException {
        //int start = (int)Thread.currentThread().getId()%this.subgraphs.length;
        //System.out.println("Partition: "+partition+" callc:"+callc);
        //callc++;
        int start = partition;
        Object ret = null;
        //int i = 0;
        //System.out.println("shard preferido: "+start);
        
        while(ret == null){
            //start = (start+1)%this.subgraphs.length;
            //int pos = start%this.subgraphs.length;
            /*if(partition == 0){
              System.out.println("partition: "+partition+" start: "+start+" callc:"+callc+" pos:"+pos);
            }*/
             //i++;
            ret =  this.subgraphs[start%this.subgraphs.length].get();
            start++;
            
            /*if(partition == 0 && ret != null){
                System.out.println("pegou!");
            }*/
        }
      
        
        /*for(int i = 0; i < this.subgraphs.length && ret == null; i++){
            ret =  this.subgraphs[i].get();
        }*/
        
        return ret; 
        
        //return this.subgraphs[partition].get();
    }

  
    
    @Override
    protected int COSInsert(Object request) throws InterruptedException {
       throw new UnsupportedOperationException(); 
               
       /* MessageContextPair mc = (MessageContextPair)request;
        ClassToThreads ct = this.pm.getClass(mc.classId);
        
        HibridLockFreeNode newvNode = new HibridLockFreeNode(request,Vertex.MESSAGE,this.subgraphs[ct.tIds[0]], subgraphs.length);
           
       
        
        
        
        if(ct.tIds.length == 1){
           
            //System.out.println("VAI INSERIR NO SHARD: "+ct.tIds[0]);
                    return this.subgraphs[ct.tIds[0]].insert(newvNode,false);
        }else {
            
            for(int i = 1; i < ct.tIds.length; i++){
                this.subgraphs[ct.tIds[i]].insertDependencies(newvNode);
                
                //System.out.println("VAI INSERIR NO SHARD: "+ct.tIds[i]);
        
            }
            //System.out.println("VAI INSERIR NO SHARD: "+ct.tIds[0]);
            System.exit(0);
            
            return this.subgraphs[ct.tIds[0]].insertNodeAndDependencies(newvNode);
            
            
            
        }*/
        
    }

    @Override
    protected int COSRemove(Object request) throws InterruptedException {
        throw new UnsupportedOperationException(); 
         /*
        HibridLockFreeNode data = ((HibridLockFreeNode) request);
        //DUVIDA: acredito que não precisa ser atomico!
        data.removedAtomic.compareAndSet(false, true);
        //int rm = data.testDepMeReady();
        //System.out.println("rm: "+rm+" space: "+this.space.availablePermits());
        return data.testDepMeReady();*/
    }
    
    
    
}
