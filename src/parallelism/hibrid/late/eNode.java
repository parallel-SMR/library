/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.late;

import parallelism.late.graph.Vertex;

/**
 *
 * @author eduardo
 */
public class eNode {

    private Vertex vertex;                      // kind of Vertex
    vNode whoIAm;                               // ref to one vNode that depends of this vNode
    eNode next;

    public eNode(vNode whoYouAre, Vertex v) {
        whoIAm = whoYouAre;
        this.vertex = v;
        next = null;
    }

    // return type of node (head, tail, ...)
    public Vertex getVertex() {
        return vertex;
    }

    // refer to vNode this eNodes represents
    public vNode getDependentVNode() {
        return whoIAm;
    }

    public eNode getNext() {
        return next;
    }

    public void setNext(eNode next) {
        this.next = next;
    }
}
