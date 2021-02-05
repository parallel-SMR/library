/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.early;

import bftsmart.tom.core.messages.TOMMessage;
import parallelism.MessageContextPair;

/**
 *
 * @author eduardo
 */
public class TOMMessageWrapper extends TOMMessage{

    public MessageContextPair msg = null;
    
    
    public TOMMessageWrapper(MessageContextPair msg) {
        this.msg = msg;
    }
    
    @Override
     public int getGroupId() {
         return msg.classId;
     }
}
