/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.late;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import parallelism.late.graph.Vertex;

/**
 *
 * @author eduardo
 */
public class HibridLockFreeNode extends vNode {

    public AtomicBoolean reservedAtomic;
    //public AtomicBoolean removedAtomic;
    public AtomicBoolean readyAtomic;

    public volatile boolean inserted = false;
    public volatile boolean removed = false;
     
    public AtomicInteger atomicCounter;

    public eNode[] headDepOn;
    public eNode[] tailDepOn;

    public ExtendedLockFreeGraph graph;

    public HibridLockFreeNode(Object data, Vertex vertex, ExtendedLockFreeGraph graph, int numParticions, int numConflic) {
        super(data, vertex, numParticions);
        reservedAtomic = new AtomicBoolean(false);
        //removedAtomic = new AtomicBoolean(false);
        readyAtomic = new AtomicBoolean(false);

        headDepOn = new eNode[numParticions];
        tailDepOn = new eNode[numParticions];

        for (int i = 0; i < numParticions; i++) {
            headDepOn[i] = new eNode(null, Vertex.HEAD);
            tailDepOn[i] = new eNode(null, Vertex.TAIL);
            headDepOn[i].setNext(tailDepOn[i]);
        }

        this.graph = graph;
        if(numConflic > 0){
           atomicCounter = new AtomicInteger(numConflic);
        }
        inserted = false;
    }

    public boolean isRemoved(){
        return removed;
    }
    
    public void insertDepOn(HibridLockFreeNode newNode, int myPartition) {
        eNode neweNode = new eNode(newNode, Vertex.MESSAGE);
        eNode aux = headDepOn[myPartition];
        while (aux.getNext().getVertex() != Vertex.TAIL) {
            aux = aux.getNext();
        }

        neweNode.setNext(tailDepOn[myPartition]);
        aux.setNext(neweNode);

    }

    public int testDepMeReady() throws InterruptedException {
        int freeNodes = 0;
        for (eNode head1 : head) {
            eNode ni = head1.getNext();
            while (ni.getVertex() != Vertex.TAIL) {
                freeNodes = freeNodes + ((HibridLockFreeNode) ni.getDependentVNode()).testReady();
                ni = ni.getNext();
            }
        }
        return freeNodes;
    }

    public int testReady() {
        if (!inserted) {
            return 0;
        }

        for (eNode head1 : headDepOn) {

            eNode ni = head1.getNext();
            while (ni.getVertex() != Vertex.TAIL) {
                if (!((HibridLockFreeNode) ni.getDependentVNode()).isRemoved()) {//not removed
                    /*if (remove) {
                    System.out.println(this.getAsRequest() + " not ready because: "
                            + ((HibridLockFreeNode) ni.getDependentVNode()).getAsRequest());
                }*/
                    return 0;
                }
                ni = ni.getNext();
            }

        }
        if (readyAtomic.compareAndSet(false, true)) { //it is necessary to return true only once
            //this.graph.addPermition();
            /*if (remove) {
                System.out.println(this.getAsRequest() + " ready for execution!");
            }*/

            this.graph.ready.release();
            return 1;
        }
        /*else {
            if (remove) {
                System.out.println(this.getAsRequest() + " ja tinha sido liberado!");
            }
        }*/
        return 0;
    }

}
