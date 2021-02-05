/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package demo.list;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.StandardStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.Storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.late.LateServiceReplica;
import parallelism.late.ConflictDefinition;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;
import parallelism.ParallelServiceReplica;
import parallelism.ParallelServiceReplica;
import parallelism.SequentialServiceReplica;
import parallelism.hibrid.HibridServiceReplica;
import parallelism.late.COSType;

public final class ListServerMP implements SingleExecutable {

    //private int interval;
    //private float maxTp = -1;
    // private boolean context;
    private int iterations = 0;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

    private long start = 0;

    private ServiceReplica replica;
    //private StateManager stateManager;
    //private ReplicaContext replicaContext;

    private List<Integer> l1 = new LinkedList<Integer>();
    private List<Integer> l2 = new LinkedList<Integer>();
    private List<Integer> l3 = new LinkedList<Integer>();
    private List<Integer> l4 = new LinkedList<Integer>();
    private List<Integer> l5 = new LinkedList<Integer>();
    private List<Integer> l6 = new LinkedList<Integer>();
    private List<Integer> l7 = new LinkedList<Integer>();
    private List<Integer> l8 = new LinkedList<Integer>();

    //private int myId;
    private PrintWriter pw;

    private boolean closed = false;

    //int exec = BFTList.ADD;
    int numberpartitions = 2;

    public ListServerMP(int id, int initThreads, int entries, int numberPartitions, boolean cbase, boolean hibrid) {

        this.numberpartitions = numberPartitions;

        if (initThreads <= 0) {
            System.out.println("Replica in sequential execution model.");

            replica = new SequentialServiceReplica(id, this, null);
        } else if (cbase) {
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
            /* ConflictDefinition cd = new ConflictDefinition() {
                @Override
                public boolean isDependent(MessageContextPair r1, MessageContextPair r2) {
                                 
                    switch (r1.opId) {
                        case MultipartitionMapping.GR:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W1
                                    || r2.opId == MultipartitionMapping.W2
                                    || r2.opId == MultipartitionMapping.W3
                                    || r2.opId == MultipartitionMapping.W4
                                    || r2.opId == MultipartitionMapping.W5
                                    || r2.opId == MultipartitionMapping.W6
                                    || r2.opId == MultipartitionMapping.W7
                                    || r2.opId == MultipartitionMapping.W8) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R1:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W1) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R2:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W2) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R3:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W3) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R4:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W4) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R5:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W5) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R6:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W6) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R7:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W7) {
                                return true;
                            }   break;
                        case MultipartitionMapping.R8:
                            if (r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.W8) {
                                return true;
                            }   break;
                        case MultipartitionMapping.GW:
                            return true;
                        case MultipartitionMapping.W1:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R1
                                    || r2.opId == MultipartitionMapping.W1) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W2:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R2
                                    || r2.opId == MultipartitionMapping.W2) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W3:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R3
                                    || r2.opId == MultipartitionMapping.W3) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W4:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R4
                                    || r2.opId == MultipartitionMapping.W4) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W5:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R5
                                    || r2.opId == MultipartitionMapping.W5) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W6:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R6
                                    || r2.opId == MultipartitionMapping.W6) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W7:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R7
                                    || r2.opId == MultipartitionMapping.W7) {
                                return true;
                            }   break;
                        case MultipartitionMapping.W8:
                            if (r2.opId == MultipartitionMapping.GR
                                    || r2.opId == MultipartitionMapping.GW
                                    || r2.opId == MultipartitionMapping.R8
                                    || r2.opId == MultipartitionMapping.W8) {
                                return true;
                            }   break;
                        default:
                            break;
                    }

                    return false;
                }
            };*/
            if (hibrid) {
                System.out.println("Replica in parallel execution model (HIBRID).");
                initThreads = initThreads * numberPartitions;
                replica = new HibridServiceReplica(id, this, null, numberPartitions, cd, initThreads);
            } else {
                System.out.println("Replica in parallel execution model (CBASE).");
                replica = new LateServiceReplica(id, this, null, initThreads, cd, COSType.lockFreeGraph, numberPartitions);

            }
        } else {
            System.out.println("Replica in parallel execution model (early).");

            if (numberPartitions == 1) {
                
                
                
                replica = new ParallelServiceReplica(id, this, null, initThreads);
            } else if (numberPartitions == 2) {
                if (initThreads == 2) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T2());
                } else if (initThreads == 4) {
                    System.out.println("4T-2S normal");
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T4TunnedW1());

                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T4TunnedR1());
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T4());
                    //System.out.println("Naive 4T-2S");
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getNaiveP2T4());
                } else if (initThreads == 8) {
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T8TunnedW1());
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T8TunnedR1());
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T8());
                } else if (initThreads == 10) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP2T10());
                } else {
                    initThreads = 12;
                    //System.out.println("CONFIGURADO PARA 12 WRITE");
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T12TunnedW1());
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T12TunnedR1());
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T12());

                    System.out.println("Vai testar RW");

                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P2T12RW());
                }
            } else if (numberPartitions == 4) {
                if (initThreads == 2) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P4T2());
                } else if (initThreads == 4) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P4T4());
                } else if (initThreads == 8) {
                    //System.out.println("Naive 8T-4S");
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P4T8());
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getNaiveP4T8());
                } else if (initThreads == 10) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP4T10());
                } else {
                    initThreads = 12;
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getM2P4T12());
                }
            } else if (numberPartitions == 6) {
                if (initThreads == 6) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP6T6());
                } else if (initThreads == 12) {
                    //System.out.println("Naive 12T-6S");
                    //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP6T12());
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getNaiveP6T12());
                } else if (initThreads == 10) {
                    replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP6T10());
                } else {
                    System.exit(0);
                }
            } else if (initThreads == 8) {
                replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP8T8());
            } else if (initThreads == 10) {
                replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP8T10());
            } else if (initThreads == 16) {
                //replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getP8T16());
                System.out.println("Naive 16T-8S");
                replica = new ParallelServiceReplica(id, this, null, initThreads, MultipartitionMapping.getNaiveP8T16());
            } else {
                System.exit(0);
            }

        }

//        this.interval = interval;
        //this.context = context;
        //this.myId = id;
        for (int i = 0; i < entries; i++) {
            l1.add(i);
            l2.add(i);
            l3.add(i);
            l4.add(i);
            l5.add(i);
            l6.add(i);
            l7.add(i);
            l8.add(i);
            //System.out.println("adicionando key: "+i);
        }

        /*try {
            File f = new File("resultado_" + id + ".txt");
            FileWriter fw = new FileWriter(f);
            pw = new PrintWriter(fw);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }*/
        System.out.println("Server initialization complete!");
    }

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream oos = new DataOutputStream(baos);

    public byte[] execute(byte[] command, MessageContext msgCtx) {

        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;

            DataInputStream dataIn = new DataInputStream(in);

            short cmd = dataIn.readShort();
            if (cmd == MultipartitionMapping.GW
                    || //global write
                    (cmd >= 21 && cmd <= 28)
                    || //single-shard write
                    cmd > 200) { //2 shards write
                //Integer value = (Integer) new ObjectInputStream(in).readObject();
                short value = dataIn.readShort();
                boolean ret = add(value, cmd);
                out = new ByteArrayOutputStream();
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeBoolean(ret);
                out.flush();
                out1.flush();
                reply = out.toByteArray();

            } else { //contains

                //Integer value = (Integer) new ObjectInputStream(in).readObject();
                short value = dataIn.readShort();
                out = new ByteArrayOutputStream();
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeBoolean(contains(value, cmd));
                out.flush();
                out1.flush();
                reply = out.toByteArray();

            }
            return reply;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    }

    public boolean add(short v, int pId) {
        int value = Short.toUnsignedInt(v);

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
                if (numberpartitions >= 4) {
                    if (!l3.contains(value)) {
                        ret = l3.add(value);
                    }
                    if (!l4.contains(value)) {
                        ret = l4.add(value);
                    }

                    if (numberpartitions >= 6) {

                        if (!l5.contains(value)) {
                            ret = l5.add(value);
                        }
                        if (!l6.contains(value)) {
                            ret = l6.add(value);
                        }

                        if (numberpartitions >= 8) {

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

    public boolean contains(short v, int pId) {
        int value = Short.toUnsignedInt(v);
        //System.out.println(value);
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

                if (this.numberpartitions == 2) {
                    l1.contains(value);
                    l2.contains(value);
                } else if (this.numberpartitions == 4) {
                    l1.contains(value);
                    l2.contains(value);
                    l3.contains(value);
                    l4.contains(value);

                } else if (this.numberpartitions == 6) {
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
            default: //conflito de contains em dois shards... pego dois qualquer (pois o add nao adiciona, dai nao tem problema)
                l1.contains(value);
                l2.contains(value);
                break;
        }
        return ret;
    }

    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: ... ListServer <processId> <number partitions> <num workers> <initial entries> <CBASE?> <hibrid scheduling?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int part = Integer.parseInt(args[1]);
        int initialNT = Integer.parseInt(args[2]);
        int entries = Integer.parseInt(args[3]);
        boolean cbase = Boolean.parseBoolean(args[4]);
        boolean h = Boolean.parseBoolean(args[5]);

        new ListServerMP(processId, initialNT, entries, part, cbase, h);
    }

}
