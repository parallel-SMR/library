/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.list;

import bftsmart.util.MultiOperationRequest;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import jdk.jshell.spi.ExecutionControl;
import parallelism.ParallelMapping;
import parallelism.hibrid.early.EarlySchedulerMapping;

/**
 *
 * @author alchieri
 */
public class BFTListMOMP<V> extends BFTList<V> {

    public EarlySchedulerMapping em = new EarlySchedulerMapping();

    public BFTListMOMP(int id, boolean parallelExecution) {
        super(id, parallelExecution);
    }

    public boolean addP1(V[] e) {
        return addFinal(e, em.getClassId(0), MultipartitionMapping.W1);
        //return addFinal(e, ParallelMapping.SYNC_ALL, MultipartitionMapping.W1);
    }

    public boolean addP2(V[] e) {
        return addFinal(e, em.getClassId(1), MultipartitionMapping.W2);
    }

    public boolean addP3(V[] e) {
        return addFinal(e, em.getClassId(2), MultipartitionMapping.W3);
    }

    public boolean addP4(V[] e) {
        return addFinal(e, em.getClassId(3), MultipartitionMapping.W4);
    }

    public boolean addP5(V[] e) {
        return addFinal(e, em.getClassId(4), MultipartitionMapping.W5);
    }

    public boolean addP6(V[] e) {
        return addFinal(e, em.getClassId(5), MultipartitionMapping.W6);
    }

    public boolean addP7(V[] e) {
        return addFinal(e, em.getClassId(6), MultipartitionMapping.W7);
    }

    public boolean addP8(V[] e) {
        return addFinal(e, em.getClassId(7), MultipartitionMapping.W8);
    }

    public boolean addFinal(V[] e, int pId, int opId) {

        MultiOperationRequest mo = new MultiOperationRequest(e.length, (short) opId);
        for (int i = 0; i < e.length; i++) {


            /* out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();
                mo.add(i, out.toByteArray(), pId, opId);*/
            short value = Short.valueOf(e[i].toString());
            mo.add(i, value);

        }
        byte[] rep = proxy.invokeParallel(mo.serialize(), pId);

        return true;

        // System.out.println("Não implementado ainda!");
        // System.exit(0);
        /*try {
            MultiOperationRequest mo = new MultiOperationRequest(e.length);
            for (int i = 0; i < e.length; i++) {
                out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();
                mo.add(i, out.toByteArray(), pId, opId);
            }
            byte[] rep = proxy.invokeParallel(mo.serialize(), -100);

            return true;

        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }*/
        //return true;
    }

    public boolean addMultiPartition(V[] e, int opId, int... partitions) {
        MultiOperationRequest mo = null;
        if (partitions.length > 0) {
            mo = new MultiOperationRequest(e.length, (short) opId);
        } else {
            mo = new MultiOperationRequest(e.length, (short) MultipartitionMapping.GW);
        }

        for (int i = 0; i < e.length; i++) {

            /* out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();*/
            short value = Short.valueOf(e[i].toString());
            mo.add(i, value);

        }
        if (partitions.length > 0) {
            byte[] rep = proxy.invokeParallel(mo.serialize(), em.getClassId(partitions));
        } else {
            byte[] rep = proxy.invokeParallel(mo.serialize(), MultipartitionMapping.GW);
        }

        return true;

        //System.out.println("Não implementado ainda!");
        // System.exit(0);
        /*try {
            MultiOperationRequest mo = new MultiOperationRequest(e.length);
            for (int i = 0; i < e.length; i++) {
                out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();
                if (partitions.length > 0) {
                    mo.add(i, out.toByteArray(), em.getClassId(partitions), opId);
                } else {
                    mo.add(i, out.toByteArray(), MultipartitionMapping.GW, MultipartitionMapping.GW);
                }
            }
            byte[] rep = proxy.invokeParallel(mo.serialize(), -100);

            return true;

        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }*/
        //return true;
    }

    public boolean containsP1(V[] e) {
        return containsFinal(e, em.getClassId(0), MultipartitionMapping.R1);
        //return containsFinal(e, ParallelMapping.CONC_ALL, MultipartitionMapping.R1);
    }

    public boolean containsP2(V[] e) {
        return containsFinal(e, em.getClassId(1), MultipartitionMapping.R2);
    }

    public boolean containsP3(V[] e) {
        return containsFinal(e, em.getClassId(2), MultipartitionMapping.R3);
    }

    public boolean containsP4(V[] e) {
        return containsFinal(e, em.getClassId(3), MultipartitionMapping.R4);
    }

    public boolean containsP5(V[] e) {
        return containsFinal(e, em.getClassId(4), MultipartitionMapping.R5);
    }

    public boolean containsP6(V[] e) {
        return containsFinal(e, em.getClassId(5), MultipartitionMapping.R6);
    }

    public boolean containsP7(V[] e) {
        return containsFinal(e, em.getClassId(6), MultipartitionMapping.R7);
    }

    public boolean containsP8(V[] e) {
        return containsFinal(e, em.getClassId(7), MultipartitionMapping.R8);
    }

    public boolean containsFinal(V[] e, int pId, short opId) {
        // try {

        MultiOperationRequest mo = new MultiOperationRequest(e.length, (short) opId);
        for (int i = 0; i < e.length; i++) {


            /* out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();
                mo.add(i, out.toByteArray(), pId, opId);*/
            short value = Short.valueOf(e[i].toString());
            mo.add(i, value);

        }
        byte[] rep = proxy.invokeParallel(mo.serialize(), pId);

        return true;
        /* } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }*/
    }

    public boolean containsMultiPartition(V[] e, int opId, int... partitions) {
        // try {
        MultiOperationRequest mo = null;
        if (partitions.length > 0) {
            mo = new MultiOperationRequest(e.length, (short) opId);
        } else {
            mo = new MultiOperationRequest(e.length, (short) MultipartitionMapping.GR);
        }

        for (int i = 0; i < e.length; i++) {

            /* out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();*/
            short value = Short.valueOf(e[i].toString());
            mo.add(i, value);

        }
        if (partitions.length > 0) {
            byte[] rep = proxy.invokeParallel(mo.serialize(), em.getClassId(partitions));
        } else {
            byte[] rep = proxy.invokeParallel(mo.serialize(), MultipartitionMapping.GR);
        }

        return true;
        /*} catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }*/
    }

}
