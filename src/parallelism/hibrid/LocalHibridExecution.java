/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hibrid;

import parallelism.hibrid.late.ExtendedLockFreeGraph;
import parallelism.hibrid.late.HibridLockFreeNode;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.util.MultiOperationRequest;
import demo.list.BFTList;
import demo.list.MultipartitionMapping;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.hibrid.early.EarlySchedulerMapping;
import parallelism.hibrid.early.HibridClassToThreads;
import parallelism.hibrid.early.HibridScheduler;
import parallelism.hibrid.early.TOMMessageWrapper;
import parallelism.late.ConflictDefinition;
import parallelism.late.graph.DependencyGraph;
import parallelism.late.graph.LockFreeGraph;
import parallelism.late.graph.Vertex;

/**
 *
 * @author eduardo
 */
public class LocalHibridExecution {

    protected List<Integer> l1 = new LinkedList<>();
    protected List<Integer> l2 = new LinkedList<>();
    protected List<Integer> l3 = new LinkedList<>();
    protected List<Integer> l4 = new LinkedList<>();
    protected List<Integer> l5 = new LinkedList<>();
    protected List<Integer> l6 = new LinkedList<>();
    protected List<Integer> l7 = new LinkedList<>();
    protected List<Integer> l8 = new LinkedList<>();

    protected int numberPartitions = 0;
    protected int maxIndex = 0;

    //protected long startTime = 0;
    protected int numRequests = 0;
    protected int numLate = 0;

    protected int pg = 0;
    protected int pl = 0;

    private boolean hibrid = false;
    private ExtendedLockFreeGraph[] subgraphs; // for hibrid test
    protected LockFreeGraph lfg = null; // for lockfree test

    //protected Semaphore space[] = null;                // counting semaphore for size of graph
    //protected Semaphore ready = new Semaphore(0);  // tells if there is ready to execute
    private HibridScheduler scheduler;

    public static void main(String[] args) {
        int entries = Integer.parseInt(args[0]);
        int lt = Integer.parseInt(args[1]);
        int np = Integer.parseInt(args[2]);
        int pG = Integer.parseInt(args[3]);
        int pW = Integer.parseInt(args[4]);
        int numReq = Integer.parseInt(args[5]);
        boolean hb = Boolean.parseBoolean(args[6]);
        int gs = Integer.parseInt(args[7]);

        /*if (hb && (lt < np)) {//nao é possível hibrido com menos threads q partições
            System.exit(0);
        }*/
        if (hb) {
            lt = lt * np;
        }

        LocalHibridExecution exec = new LocalHibridExecution(entries, lt, np, pG, pW, numReq, gs, hb);

        LateWorker[] w = exec.init();
        int seconds = 20;
        try {
            Thread.sleep(seconds * 1000);//20 segundos
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        int total = 0;
        for (int i = 0; i < w.length; i++) {
            total = total + w[i].processed;
        }

        long tp = total / seconds;
        System.out.println("TP [partitions: " + np + ", late workers: " + lt + "] :" + tp);
        String filePath = null;
        if (hb) {
            filePath = "hibrid_" + np + "_" + lt + "_" + entries + "_" + pG + "_" + pW + ".txt";
        } else {
            filePath = "lockfree_" + np + "_" + lt + "_" + entries + "_" + pG + "_" + pW + ".txt";
        }
        PrintWriter pw;
        try {
            pw = new PrintWriter(new FileWriter(new File(filePath)));
            pw.println(np + " " + lt + " " + tp);
            pw.flush();
            pw.close();
        } catch (IOException ex) {
            Logger.getLogger(LocalHibridExecution.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.exit(0);
    }

    public LocalHibridExecution(int entries, int lateThreads, int numberPartitions, int pG, int pW, int numReqs, int graphsize, boolean hibrid) {
        this.hibrid = hibrid;
        this.pg = pG;
        this.pl = pW;
        this.numberPartitions = numberPartitions;
        this.numLate = lateThreads;
        this.scheduler = new HibridScheduler(numberPartitions,
                new EarlySchedulerMapping().generateMappings(numberPartitions), numReqs / numberPartitions);
        this.maxIndex = entries;
        this.numRequests = numReqs;

        ConflictDefinition cd = new ConflictDefinition() {

            private boolean conflictR1(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W1
                        || (MultipartitionMapping.W12 <= opId && opId <= MultipartitionMapping.W18);
            }

            private boolean conflictW1(int opId) {
                return conflictR1(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R1
                        || (MultipartitionMapping.R12 <= opId && opId <= MultipartitionMapping.R18);
            }

            private boolean conflictR2(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W2
                        || opId == MultipartitionMapping.W12
                        || (MultipartitionMapping.W23 <= opId && opId <= MultipartitionMapping.W28);
            }

            private boolean conflictW2(int opId) {
                return conflictR2(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R2
                        || opId == MultipartitionMapping.R12
                        || (MultipartitionMapping.R23 <= opId && opId <= MultipartitionMapping.R28);
            }

            private boolean conflictR3(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W3
                        || opId == MultipartitionMapping.W13
                        || opId == MultipartitionMapping.W23
                        || (MultipartitionMapping.W34 <= opId && opId <= MultipartitionMapping.W38);
            }

            private boolean conflictW3(int opId) {
                return conflictR3(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R3
                        || opId == MultipartitionMapping.R13
                        || opId == MultipartitionMapping.R23
                        || (MultipartitionMapping.R34 <= opId && opId <= MultipartitionMapping.R38);
            }

            private boolean conflictR4(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W4
                        || opId == MultipartitionMapping.W14
                        || opId == MultipartitionMapping.W24
                        || opId == MultipartitionMapping.W34
                        || (MultipartitionMapping.W45 <= opId && opId <= MultipartitionMapping.W48);
            }

            private boolean conflictW4(int opId) {
                return conflictR4(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R4
                        || opId == MultipartitionMapping.R14
                        || opId == MultipartitionMapping.R24
                        || opId == MultipartitionMapping.R34
                        || (MultipartitionMapping.R45 <= opId && opId <= MultipartitionMapping.R48);
            }

            private boolean conflictR5(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W5
                        || opId == MultipartitionMapping.W15
                        || opId == MultipartitionMapping.W25
                        || opId == MultipartitionMapping.W35
                        || opId == MultipartitionMapping.W45
                        || (MultipartitionMapping.W56 <= opId && opId <= MultipartitionMapping.W58);
            }

            private boolean conflictW5(int opId) {
                return conflictR5(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R5
                        || opId == MultipartitionMapping.R15
                        || opId == MultipartitionMapping.R25
                        || opId == MultipartitionMapping.R35
                        || opId == MultipartitionMapping.R45
                        || (MultipartitionMapping.R56 <= opId && opId <= MultipartitionMapping.R58);
            }

            private boolean conflictR6(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W6
                        || opId == MultipartitionMapping.W16
                        || opId == MultipartitionMapping.W26
                        || opId == MultipartitionMapping.W36
                        || opId == MultipartitionMapping.W46
                        || opId == MultipartitionMapping.W56
                        || opId == MultipartitionMapping.W67
                        || opId == MultipartitionMapping.W68;
            }

            private boolean conflictW6(int opId) {
                return conflictR6(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R6
                        || opId == MultipartitionMapping.R16
                        || opId == MultipartitionMapping.R26
                        || opId == MultipartitionMapping.R36
                        || opId == MultipartitionMapping.R46
                        || opId == MultipartitionMapping.R56
                        || opId == MultipartitionMapping.R67
                        || opId == MultipartitionMapping.R68;
            }

            private boolean conflictR7(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W7
                        || opId == MultipartitionMapping.W17
                        || opId == MultipartitionMapping.W27
                        || opId == MultipartitionMapping.W37
                        || opId == MultipartitionMapping.W47
                        || opId == MultipartitionMapping.W57
                        || opId == MultipartitionMapping.W67
                        || opId == MultipartitionMapping.W78;
            }

            private boolean conflictW7(int opId) {
                return conflictR7(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R7
                        || opId == MultipartitionMapping.R17
                        || opId == MultipartitionMapping.R27
                        || opId == MultipartitionMapping.R37
                        || opId == MultipartitionMapping.R47
                        || opId == MultipartitionMapping.R57
                        || opId == MultipartitionMapping.R67
                        || opId == MultipartitionMapping.R78;
            }

            private boolean conflictR8(int opId) {
                return opId == MultipartitionMapping.GW
                        || opId == MultipartitionMapping.W8
                        || opId == MultipartitionMapping.W18
                        || opId == MultipartitionMapping.W28
                        || opId == MultipartitionMapping.W38
                        || opId == MultipartitionMapping.W48
                        || opId == MultipartitionMapping.W58
                        || opId == MultipartitionMapping.W68
                        || opId == MultipartitionMapping.W78;
            }

            private boolean conflictW8(int opId) {
                return conflictR8(opId)
                        || opId == MultipartitionMapping.GR
                        || opId == MultipartitionMapping.R8
                        || opId == MultipartitionMapping.R18
                        || opId == MultipartitionMapping.R28
                        || opId == MultipartitionMapping.R38
                        || opId == MultipartitionMapping.R48
                        || opId == MultipartitionMapping.R58
                        || opId == MultipartitionMapping.R68
                        || opId == MultipartitionMapping.R78;
            }

            @Override
            public boolean isDependent(MessageContextPair r1, MessageContextPair r2) {

                switch (r1.opId) {
                    case MultipartitionMapping.GR:
                        if (conflictR1(r2.opId)
                                || conflictR2(r2.opId)
                                || conflictR3(r2.opId)
                                || conflictR4(r2.opId)
                                || conflictR5(r2.opId)
                                || conflictR6(r2.opId)
                                || conflictR7(r2.opId)
                                || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R1:
                        if (conflictR1(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R2:
                        if (conflictR2(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R3:
                        if (conflictR3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R4:
                        if (conflictR4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R5:
                        if (conflictR5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R6:
                        if (conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R7:
                        if (conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R8:
                        if (conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.GW:
                        return true;
                    case MultipartitionMapping.W1:
                        if (conflictW1(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W2:
                        if (conflictW2(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W3:
                        if (conflictW3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W4:
                        if (conflictW4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W5:
                        if (conflictW5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W6:
                        if (conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W7:
                        if (conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W8:
                        if (conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R12:
                        if (conflictR1(r2.opId) || conflictR2(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R13:
                        if (conflictR1(r2.opId) || conflictR3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R14:
                        if (conflictR1(r2.opId) || conflictR4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R15:
                        if (conflictR1(r2.opId) || conflictR5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R16:
                        if (conflictR1(r2.opId) || conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R17:
                        if (conflictR1(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R18:
                        if (conflictR1(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R23:
                        if (conflictR2(r2.opId) || conflictR3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R24:
                        if (conflictR2(r2.opId) || conflictR4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R25:
                        if (conflictR2(r2.opId) || conflictR5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R26:
                        if (conflictR2(r2.opId) || conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R27:
                        if (conflictR2(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R28:
                        if (conflictR2(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R34:
                        if (conflictR3(r2.opId) || conflictR4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R35:
                        if (conflictR3(r2.opId) || conflictR5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R36:
                        if (conflictR3(r2.opId) || conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R37:
                        if (conflictR3(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R38:
                        if (conflictR3(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R45:
                        if (conflictR4(r2.opId) || conflictR5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R46:
                        if (conflictR4(r2.opId) || conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R47:
                        if (conflictR4(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R48:
                        if (conflictR4(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R56:
                        if (conflictR5(r2.opId) || conflictR6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R57:
                        if (conflictR5(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R58:
                        if (conflictR5(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R67:
                        if (conflictR6(r2.opId) || conflictR7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R68:
                        if (conflictR6(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.R78:
                        if (conflictR7(r2.opId) || conflictR8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W12:
                        if (conflictW1(r2.opId) || conflictW2(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W13:
                        if (conflictW1(r2.opId) || conflictW3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W14:
                        if (conflictW1(r2.opId) || conflictW4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W15:
                        if (conflictW1(r2.opId) || conflictW5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W16:
                        if (conflictW1(r2.opId) || conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W17:
                        if (conflictW1(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W18:
                        if (conflictW1(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W23:
                        if (conflictW2(r2.opId) || conflictW3(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W24:
                        if (conflictW2(r2.opId) || conflictW4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W25:
                        if (conflictW2(r2.opId) || conflictW5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W26:
                        if (conflictW2(r2.opId) || conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W27:
                        if (conflictW2(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W28:
                        if (conflictW2(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W34:
                        if (conflictW3(r2.opId) || conflictW4(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W35:
                        if (conflictW3(r2.opId) || conflictW5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W36:
                        if (conflictW3(r2.opId) || conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W37:
                        if (conflictW3(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W38:
                        if (conflictW3(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W45:
                        if (conflictW4(r2.opId) || conflictW5(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W46:
                        if (conflictW4(r2.opId) || conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W47:
                        if (conflictW4(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W48:
                        if (conflictW4(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W56:
                        if (conflictW5(r2.opId) || conflictW6(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W57:
                        if (conflictW5(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W58:
                        if (conflictW5(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W67:
                        if (conflictW6(r2.opId) || conflictW7(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W68:
                        if (conflictW6(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;
                    case MultipartitionMapping.W78:
                        if (conflictW7(r2.opId) || conflictW8(r2.opId)) {
                            return true;
                        }
                        break;

                    default:
                        break;
                }

                return false;
            }
        };
        if (hibrid) {
            this.subgraphs = new ExtendedLockFreeGraph[numberPartitions];
            for (int i = 0; i < numberPartitions; i++) {
                this.subgraphs[i] = new ExtendedLockFreeGraph(cd, i, graphsize / numberPartitions);
            }
        } else {
            this.lfg = new LockFreeGraph(graphsize, cd);
        }

        for (int i = 0; i < entries; i++) {
            l1.add(i);
            l2.add(i);
            l3.add(i);
            l4.add(i);
            l5.add(i);
            l6.add(i);
            l7.add(i);
            l8.add(i);
        }
    }

    public LateWorker[] init() {
        LateWorker[] lw = initLateWorkers(numLate, numberPartitions);
        if (hibrid) {
            initEarlyWorkers(numberPartitions);
        }
        Client c = new Client(pg, pl, numRequests);
        //startTime = System.nanoTime();
        c.start();
        return lw;
    }

    private class Client extends Thread {

        int pGlobal = 0;
        int pW = 0;
        int numRequests = 0;
        MessageContextPair[] reqs = null;

        public Client(int pG, int pW, int numRequests) {
            this.pGlobal = pG;
            this.pW = pW;
            this.numRequests = numRequests;
            createRequests();
        }

        private void createRequests() {
            reqs = new MessageContextPair[numRequests];
            Random randOp = new Random();
            Random randPart = new Random();
            Random randGlobal = new Random();
            Random indexRand = new Random();
            Random confAll = new Random();
            Random partitonRand = new Random();

            EarlySchedulerMapping em = new EarlySchedulerMapping();
            int[] allPartitions = new int[numberPartitions];
            for (int i = 0; i < numberPartitions; i++) {
                allPartitions[i] = i;
            }
            int[] conf = new int[2];
            for (int i = 0; i < reqs.length; i++) {
                int g = randGlobal.nextInt(100);
                int r = randOp.nextInt(100);
                int p = 0;
                int op = 0;
                int all = 0;

                if (numberPartitions > 1 && g < this.pGlobal) {//global
                    if (r < pW) {
                        //GW
                        p = -1;
                    } else {
                        //GR;
                        p = -2;
                    }

                    all = confAll.nextInt(100);
                    if (all >= 10) {
                        if (numberPartitions == 2) {
                            conf[0] = 0;
                            conf[1] = 1;
                        } else {
                            conf[0] = partitonRand.nextInt(numberPartitions);
                            do {
                                conf[1] = partitonRand.nextInt(numberPartitions);
                            } while (conf[0] == conf[1]);
                            Arrays.sort(conf);
                        }
                    }
                } else {//local
                    if (r < pW) {
                        op = BFTList.ADD;
                    } else {
                        op = BFTList.CONTAINS;
                    }
                }

                if (p == 0) {
                    switch (numberPartitions) {
                        case 1:
                            //1 partition
                            p = 1;
                            break;
                        case 2:
                            //2 partitions
                            r = randPart.nextInt(100);
                            if (r < 50) {
                                p = 1;
                            } else {
                                p = 2;
                            }
                            break;
                        case 4:
                            //4 partitions
                            r = randPart.nextInt(100);
                            if (r < 25) {
                                p = 1;
                            } else if (r < 50) {
                                p = 2;
                            } else if (r < 75) {
                                p = 3;
                            } else {
                                p = 4;
                            }
                            break;
                        case 6:
                            //6 partitions
                            r = randPart.nextInt(60);
                            if (r < 10) {
                                p = 1;
                            } else if (r < 20) {
                                p = 2;
                            } else if (r < 30) {
                                p = 3;
                            } else if (r < 40) {
                                p = 4;
                            } else if (r < 50) {
                                p = 5;
                            } else {
                                p = 6;
                            }
                            break;
                        default:
                            //8 partitions
                            r = randPart.nextInt(80);
                            if (r < 10) {
                                p = 1;
                            } else if (r < 20) {
                                p = 2;
                            } else if (r < 30) {
                                p = 3;
                            } else if (r < 40) {
                                p = 4;
                            } else if (r < 50) {
                                p = 5;
                            } else if (r < 60) {
                                p = 6;
                            } else if (r < 70) {
                                p = 7;
                            } else {
                                p = 8;
                            }
                            break;
                    }
                }

                TOMMessage tm = null;
                if (i == reqs.length - 1) {
                    tm = new TOMMessage();
                }

                if (p == -1) { //GW
                    //int index = maxIndex - 1;
                    if (all >= 10) {
                        int opId = 200 + (conf[0] + 1) * 10 + (conf[1] + 1);
                        reqs[i] = new MessageContextPair(tm, em.getClassId(conf),
                                indexRand.nextInt(maxIndex), (short) 0, (short) opId, null);
                    } else {
                        reqs[i] = new MessageContextPair(tm, em.getClassId(allPartitions),
                                indexRand.nextInt(maxIndex), (short) 0, (short) MultipartitionMapping.GW, null);
                    }
                } else if (p == -2) {//GR
                    //int index = maxIndex - 1;
                    if (all >= 10) {
                        int opId = 100 + (conf[0] + 1) * 10 + (conf[1] + 1);
                        reqs[i] = new MessageContextPair(tm, em.getClassId(conf),
                                indexRand.nextInt(maxIndex), (short) 0, (short) opId, null);
                    } else {
                        reqs[i] = new MessageContextPair(tm, em.getClassId(allPartitions),
                                indexRand.nextInt(maxIndex), (short)0, (short) MultipartitionMapping.GR, null);
                    }
                } else if (op == BFTList.ADD) {
                    switch (p) {
                        case 1:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(0), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W1, null);
                            break;
                        case 2:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(1), indexRand.nextInt(maxIndex),(short) 0, MultipartitionMapping.W2, null);
                            break;
                        case 3:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(2), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W3, null);
                            break;
                        case 4:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(3), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W4, null);
                            break;
                        case 5:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(4), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W5, null);
                            break;
                        case 6:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(5), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W6, null);
                            break;
                        case 7:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(6), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W7, null);
                            break;
                        default:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(7), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.W8, null);
                            break;
                    }

                } else if (op == BFTList.CONTAINS) {

                    switch (p) {
                        case 1:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(0), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R1, null);
                            break;
                        case 2:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(1), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R2, null);
                            break;
                        case 3:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(2), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R3, null);
                            break;
                        case 4:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(3), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R4, null);
                            break;
                        case 5:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(4), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R5, null);
                            break;
                        case 6:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(5), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R6, null);
                            break;
                        case 7:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(6), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R7, null);
                            break;
                        default:
                            reqs[i] = new MessageContextPair(tm, em.getClassId(7), indexRand.nextInt(maxIndex), (short)0, MultipartitionMapping.R8, null);
                            break;
                    }

                }

            }

            System.out.println("Criacao das requisições finalizada!");
            //statistics.start();
        }

        @Override
        public void run() {

            if (hibrid) {
                for (int i = 0; i < reqs.length; i++) {
                    scheduler.schedule(reqs[i]);
                }
            } else {
                for (int i = 0; i < reqs.length; i++) {

                    try {
                        lfg.insert(reqs[i]);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }

            }

            System.out.println("Fim do envio!");

        }

    }

    private void initEarlyWorkers(int numPartitions) {
        System.out.println("n early: " + numPartitions);
        int tid = 0;
        for (int i = 0; i < numPartitions; i++) {
            new EarlyWorker(tid, this.scheduler.getAllQueues()[i]).start();
            tid++;
        }
    }

    private class EarlyWorker extends Thread {

        private int thread_id;
        private Queue<TOMMessage> reqs = null;

        public EarlyWorker(int id, Queue<TOMMessage> reqs) {
            this.thread_id = id;
            this.reqs = reqs;
        }

        public void run() {
            while (true) {
                TOMMessageWrapper request = (TOMMessageWrapper) reqs.poll();
                if (request != null) {
                    HibridClassToThreads ct = ((HibridScheduler) scheduler).getClass(request.getGroupId());
                    if (ct.type == HibridClassToThreads.CONC) {
                        
                            subgraphs[thread_id].insert(new HibridLockFreeNode(request.msg,
                                    Vertex.MESSAGE, subgraphs[thread_id], subgraphs.length, 0), false, false);

                        

                    } else if (ct.type == HibridClassToThreads.SYNC) {
                        TOMMessageWrapper mw = (TOMMessageWrapper) request;

                        if (mw.msg.threadId == thread_id) {
                            mw.msg.node.graph = subgraphs[thread_id];
                            subgraphs[thread_id].insert(mw.msg.node, false, true);
                        } else {
                            subgraphs[thread_id].insert(mw.msg.node, true, true);
                        }
                    }
                }
            }
        }
    }

    private LateWorker[] initLateWorkers(int total, int partitions) {

        System.out.println("n late: " + total);
        int tid = 0;
        LateWorker[] ret = new LateWorker[total];
        if (hibrid) {
            for (int i = 0; i < total; i++) {
                ret[i] = new LateWorker(tid, partitions);
                ret[i].start();
                tid++;
            }
        } else {
            for (int i = 0; i < total; i++) {
                ret[i] = new LFLateWorker(tid, partitions);
                ret[i].start();
                tid++;
            }
        }
        return ret;
    }

    private class LFLateWorker extends LateWorker {

        public LFLateWorker(int id, int partitions) {
            super(id, partitions);
        }

        public void run() {

            while (true) {
                try {
                    Object node = lfg.get();

                    execute(((DependencyGraph.vNode) node).getAsRequest());

                    lfg.remove(node);
                    processed++;
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

    }

    private class LateWorker extends Thread {

        protected int thread_id;
        protected int myPartition = 0;
        protected int numPartitions;

        public int processed = 0;

        public LateWorker(int id, int partitions) {
            this.thread_id = id;
            this.myPartition = thread_id % partitions;
            //System.out.println("my partition: " + this.myPartition);
            this.numPartitions = partitions;
        }

        public void run() {

            while (true) {
                try {
                    HibridLockFreeNode node = subgraphs[this.myPartition].get();

                    execute(node.getAsRequest());

                    subgraphs[this.myPartition].remove(node);

                    processed++;

                    /* if (node.getAsRequest().request != null) {
                        long end = System.nanoTime();

                        long total = (end - startTime) / 1000000000;
                        long tp = numRequests / total;
                        System.out.println("TP [partitions: " + numPartitions + ", late workers: " + numLate + "] :" + tp);
                        String filePath = "hibrid_" + numPartitions + "_" + numLate + "_" + maxIndex + "_" + pg + "_" + pl + ".txt";
                        PrintWriter pw = new PrintWriter(new FileWriter(new File(filePath)));
                        pw.println(numPartitions + " " + numLate + " " + tp);
                        pw.flush();
                        pw.close();
                        System.exit(0);
                    }*/
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

        }

        public boolean add(int value, int pId) {
            //System.out.println("VAI EXECUTAR ADD "+pId);
            boolean ret = false;
            switch (pId) {
                case MultipartitionMapping.W1:
                    if (!l1.contains(value)) {
                        ret = l1.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W2:
                    if (!l2.contains(value)) {
                        ret = l2.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W3:
                    if (!l3.contains(value)) {
                        ret = l3.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W4:
                    if (!l4.contains(value)) {
                        ret = l4.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W5:
                    if (!l5.contains(value)) {
                        ret = l5.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W6:
                    if (!l6.contains(value)) {
                        ret = l6.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W7:
                    if (!l7.contains(value)) {
                        ret = l7.add(value);
                    }
                    return ret;
                case MultipartitionMapping.W8:
                    if (!l8.contains(value)) {
                        ret = l8.add(value);
                    }
                    return ret;
                case MultipartitionMapping.GW:
                    if (!l1.contains(value)) {
                        ret = l1.add(value);
                    }
                    if (!l2.contains(value)) {
                        ret = l2.add(value);
                    }
                    if (numPartitions >= 4) {
                        if (!l3.contains(value)) {
                            ret = l3.add(value);
                        }
                        if (!l4.contains(value)) {
                            ret = l4.add(value);
                        }

                        if (numPartitions >= 6) {

                            if (!l5.contains(value)) {
                                ret = l5.add(value);
                            }
                            if (!l6.contains(value)) {
                                ret = l6.add(value);
                            }

                            if (numPartitions >= 8) {

                                if (!l7.contains(value)) {
                                    ret = l7.add(value);
                                }
                                if (!l8.contains(value)) {
                                    ret = l8.add(value);
                                }
                            }
                        }
                    }

                    return ret;
                default: //conflito de add em dois shards... pego dois qualquer, pois não adiciona por causa do contains
                    if (!l1.contains(value)) {
                        ret = l1.add(value);
                    }
                    if (!l2.contains(value)) {
                        ret = l2.add(value);
                    }
                    break;
            }
            return ret;
        }

        public boolean contains(int value, int pId) {
            boolean ret = false;
            switch (pId) {
                case MultipartitionMapping.R1:
                    l1.contains(value);
                    break;
                case MultipartitionMapping.R2:
                    l2.contains(value);
                    break;
                case MultipartitionMapping.R3:
                    l3.contains(value);
                    break;
                case MultipartitionMapping.R4:
                    l4.contains(value);
                    break;
                case MultipartitionMapping.R5:
                    l5.contains(value);
                    break;
                case MultipartitionMapping.R6:
                    l6.contains(value);
                    break;
                case MultipartitionMapping.R7:
                    l7.contains(value);
                    break;
                case MultipartitionMapping.R8:
                    l8.contains(value);
                    break;
                case MultipartitionMapping.GR:

                    if (this.numPartitions == 2) {
                        l1.contains(value);
                        l2.contains(value);
                    } else if (this.numPartitions == 4) {
                        l1.contains(value);
                        l2.contains(value);
                        l3.contains(value);
                        l4.contains(value);

                    } else if (this.numPartitions == 6) {
                        l1.contains(value);
                        l2.contains(value);
                        l3.contains(value);
                        l4.contains(value);
                        l5.contains(value);
                        l6.contains(value);
                    } else { // 8 partitions 
                        l1.contains(value);
                        l2.contains(value);
                        l3.contains(value);
                        l4.contains(value);
                        l5.contains(value);
                        l6.contains(value);
                        l7.contains(value);
                        l8.contains(value);
                    }
                    break;
                default: //conflito de add em dois shards... pego dois qualquer, pois não adiciona por causa do contains
                    l1.contains(value);
                    l2.contains(value);
                    break;
            }
            return ret;
        }

        public void execute(MessageContextPair r) {
            if (r.opId == MultipartitionMapping.GW
                    || //global write
                    (r.opId >= 21 && r.opId <= 28)
                    || //single-shard write
                    r.opId > 200) { //2 shards write
                add(r.index, r.opId);
            } else { //contains
                contains(r.index, r.opId);
            }

        }

    }

}
