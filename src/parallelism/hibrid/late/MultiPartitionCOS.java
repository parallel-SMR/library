/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.late;

import java.util.concurrent.Semaphore;
import parallelism.late.ConflictDefinition;
import parallelism.late.graph.COS;

/**
 *
 * @author eduardo
 */
public abstract class MultiPartitionCOS extends COS{

    //private Semaphore[] readyByPartition;
    
    public MultiPartitionCOS(int limit, ConflictDefinition cd, int partitions) {
        super(limit, cd);
       /* readyByPartition = new Semaphore[partitions];
        for(int i = 0; i < readyByPartition.length; i++){
            readyByPartition[i] = new Semaphore(0);
        }*/
        
    }
    
    @Override
     public void insert(Object request) throws InterruptedException {
        space.acquire();
        //System.out.println("disponiveis: "+space.availablePermits());
        int readyNum = 
                COSInsert(request);
        this.ready.release(readyNum);
    }
    
    @Override
    public void remove(Object requestNode) throws InterruptedException {
        int readyNum = 
                COSRemove(requestNode);
        this.space.release();
        this.ready.release(readyNum);
    }

    public Object get(int partition) throws InterruptedException {
        //this.readyByPartition[partition].acquire();
        //System.out.println("VAI PEGAR PERMISSAO PARA EXECUTAR ");
        this.ready.acquire();
        //System.out.println("PEGOU PERMISSAO PARA EXECUTAR ");
        
        return COSGet(partition);
    }
    
    
    
    /*protected void putPermition(int partition){
        this.readyByPartition[partition].release();
    }*/

    @Override
    protected Object COSGet() throws InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
     
    
    protected abstract Object COSGet(int partition) throws InterruptedException;

    
}
