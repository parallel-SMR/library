/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid.early;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import parallelism.ClassToThreads;

/**
 *
 * @author eduardo
 */
public class EarlySchedulerMapping {

    public HibridClassToThreads[] CtoT = null;

    public int[] partitions;
    
    public EarlySchedulerMapping() {
    }

    public HibridClassToThreads[] generateMappings(int numPartitions) {
        partitions = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = i;
        }
        return generateMappings(partitions);
    }

    public HibridClassToThreads[] generateMappings(int... partitions) {
        this.partitions = partitions;
        generate(partitions);
        /* for(int i = 0; i < CtoT.length;i++){
            System.out.println(CtoT[i]);
        }*/
        return CtoT;
    }

    public int getClassId(int... partitions) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < partitions.length; j++) {
            //System.out.print(iv[j]);
            sb.append(partitions[j]);
        }

        //System.out.print("getClassId "+sb.toString().hashCode());
        
        return sb.toString().hashCode();
    }

   
    
    /*public static void main(String[] args) {
        EarlySchedulerMapping em = new EarlySchedulerMapping();
        em.generateMappings(0,1);
        
        int id = em.getClassId(0,1);
        System.out.println("id: "+id);
        
        id = em.getClassId(0);
        System.out.println("id: "+id);
    }*/
    
    
    public void generate(int[] status) {
        //int[] status = new int[]{0, 1, 2, 3}; //aqui pode ser qualquer objeto que implemente Comparable
        List<SortedSet<Comparable>> allCombList = new ArrayList<SortedSet<Comparable>>(); //aqui vai ficar a resposta

        for (int nstatus : status) {
            allCombList.add(new TreeSet<Comparable>(Arrays.asList(nstatus))); //insiro a combinação "1 a 1" de cada item
        }

        for (int nivel = 1; nivel < status.length; nivel++) {
            List<SortedSet<Comparable>> statusAntes = new ArrayList<SortedSet<Comparable>>(allCombList); //crio uma cópia para poder não iterar sobre o que já foi
            for (Set<Comparable> antes : statusAntes) {
                SortedSet<Comparable> novo = new TreeSet<Comparable>(antes); //para manter ordenado os objetos dentro do set
                novo.add(status[nivel]);
                if (!allCombList.contains(novo)) { //testo para ver se não está repetido
                    allCombList.add(novo);
                }
            }
        }

        Collections.sort(allCombList, new Comparator<SortedSet<Comparable>>() { //aqui só para organizar a saída de modo "bonitinho"

            @Override
            public int compare(SortedSet<Comparable> o1, SortedSet<Comparable> o2) {
                int sizeComp = o1.size() - o2.size();
                if (sizeComp == 0) {
                    Iterator<Comparable> o1iIterator = o1.iterator();
                    Iterator<Comparable> o2iIterator = o2.iterator();
                    while (sizeComp == 0 && o1iIterator.hasNext()) {
                        sizeComp = o1iIterator.next().compareTo(o2iIterator.next());
                    }
                }
                return sizeComp;

            }
        });

        //System.out.println(allCombList);
        //Iterator i = allCombList.iterator();

        this.CtoT = new HibridClassToThreads[allCombList.size()];
        for (int i = 0; i < this.CtoT.length; i++) {
            Object[] ar = ((TreeSet) allCombList.get(i)).toArray();
            int[] ids = new int[ar.length];
            for (int j = 0; j < ids.length; j++) {
                ids[j] = Integer.parseInt(ar[j].toString());
            }
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < ids.length; j++) {
                //System.out.print(iv[j]);
                sb.append(ids[j]);
            }
           // System.out.println(sb.toString().hashCode());
            int type = ClassToThreads.CONC;
            if (ids.length > 1) {
                type = ClassToThreads.SYNC;
            }

            this.CtoT[i] = new HibridClassToThreads(sb.toString().hashCode(), type, ids);
        }
        /*while (i.hasNext()) {
            Object[] ar = ((TreeSet) i.next()).toArray();
            int[] iv = new int[ar.length];
            for (int j = 0; j < iv.length; j++) {
                iv[j] = Integer.parseInt(ar[j].toString());
            }
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < iv.length; j++) {
                //System.out.print(iv[j]);
                sb.append(iv[j]);
            }

            System.out.println(sb.toString().hashCode());
            
        }*/
    }

}
