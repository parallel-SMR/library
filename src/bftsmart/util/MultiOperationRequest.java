/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardo
 */
public class MultiOperationRequest {

    public short[] operations;
    public short opId;

    public MultiOperationRequest(int number, short opId) {
        this.operations = new short[number];
        this.opId = opId;
    }

    public void add(int index, short data){
        this.operations[index] = data;
    }
    
    public MultiOperationRequest(byte[] buffer) {
        DataInputStream dis = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(buffer);
            dis = new DataInputStream(in);
            
            this.opId = dis.readShort();
            
            this.operations = new short[dis.readShort()];
            
            for(int i = 0; i < this.operations.length; i++){
                this.operations[i] = dis.readShort();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                dis.close();
            } catch (IOException ex) {
                Logger.getLogger(MultiOperationRequest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
		

    }

    public byte[] serialize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream oos = new DataOutputStream(baos);
            
            oos.writeShort(opId);
            
            oos.writeShort(operations.length);
            
            for(int i = 0; i < operations.length; i++){
                oos.writeShort(this.operations[i]);
            }
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

   /* public class Operation {

        public byte[] data;
        public int classId;
        public int opId;
        
        public Operation() {
        }

        public Operation(byte[] data, int classId, int opId) {
            this.data = data;
            this.classId = classId;
            this.opId = opId;
        }

        
        

    }*/

}
