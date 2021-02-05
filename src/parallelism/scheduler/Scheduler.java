/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.scheduler;

import bftsmart.tom.core.messages.TOMMessage;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;


/**
 *
 * @author eduardo
 */
public interface Scheduler {
    public void schedule(MessageContextPair request);
    public void schedule(TOMMessage request);
    public ParallelMapping getMapping();
    public void scheduleReplicaReconfiguration();
    public int getNumWorkers();
}
