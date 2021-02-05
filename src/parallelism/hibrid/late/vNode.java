/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.late;

import parallelism.MessageContextPair;
import parallelism.late.graph.Vertex;

/**
 *
 * @author eduardo
 */
public class vNode {
      private Vertex vertex;                          // kind of Vertex
        private Object data;                               // the item kept in the graph node
        //private int depends;                            // number of nodes it depends of
        vNode next;                                     // next in the linked list
       // private boolean reserved;                       // if reserved for execution
        
        eNode[] head;
        eNode[] tail;

        //final private ReentrantLock lock;
        
        
        public vNode(Object data, Vertex vertex, int partitions) {
            this.data = data;                           // DATA and kind kept
            this.vertex = vertex;                       //
           
            head = new eNode[partitions];
            tail = new eNode[partitions];

            for(int i = 0; i< partitions; i++){
                head[i] = new eNode(null, Vertex.HEAD);        // empty list of nodes that
                tail[i] = new eNode(null, Vertex.TAIL);        // DEPEND FROM THIS NODE
                head[i].setNext(tail[i]);
            }
            this.next = null;                           // LINKING
            
        }
        
         // DATA kept in node and kind of node (head, tail, message)
        public MessageContextPair getAsRequest() {
            return (MessageContextPair) data;
        }

        public Object getData() {
            return data;
        }
        
        
        public Vertex getVertex() {
            return vertex;
        }

        // LINKING info and setting
        public void setNext(vNode next) {
            this.next = next;
        }

        public vNode getNext() {
            return next;
        }

            
        // adds one more node that depends of this one
        public void insert(vNode newNode, int partition) {
            eNode neweNode = new eNode(newNode, Vertex.MESSAGE);
            eNode aux = head[partition];
            while (aux.getNext().getVertex() != Vertex.TAIL) {
                aux = aux.getNext();
            }
            neweNode.setNext(tail[partition]);
            aux.setNext(neweNode);
        }

}
