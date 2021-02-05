/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardo
 */
public class ConsolidateResults {

    public ConsolidateResults() {
    }

    public static void main(String[] args) {
        String[] ids = new String[3];
        ids[0] = "0";
        ids[1] = "1";
        ids[2] = "2";

        String[] part = new String[1];
        part[0] = "1";
        /*part[1] = "2";
        part[2] = "4";
        part[3] = "6";
        part[4] = "8";
*/
        String[] w = new String[14];
        w[0] = "1";
        w[1] = "2";
        w[2] = "4";
        w[3] = "6";
        w[4] = "8";
        w[5] = "10";
        w[6] = "12";
        w[7] = "16";
        w[8] = "24";
        w[9] = "32";
        w[10] = "40";
        w[11] = "48";
        w[12] = "56";
        w[13] = "64";

        new ConsolidateResults().consolidade1shard(ids, part, w);
        //new ConsolidateResults().consolidade1(part, w);
    }

    public void consolidade1(String[] partitions, String[] workers) {
        try {
            PrintWriter pw = null;
            
            pw = new PrintWriter(new FileWriter(new File("resultsG_5_5.txt")));
            
           
            
            for (String partition : partitions) {
                String line = partition;
                                
                
                for (String w : workers) {
                    String pathL = "results_g/lockfree_" + partition + "_" + w + "_1000_5_5.txt";
                    double tp = loadTP1(pathL);
                    line = line + " " + tp;
                }
                
                System.out.println(line);
                
                for (String w : workers) {
                    int nw=Integer.parseInt(w)*Integer.parseInt(partition);
                    String pathL = "results_g/hibrid_" + partition + "_" + nw + "_1000_5_5.txt";
                    double tp = loadTP1(pathL);
                    line = line + " " + tp;
                    
                }
                
                System.out.println(line);
                
                pw.println(line);
                pw.flush();
                
            }
            
            pw.close();
            pw.close();
        } catch (IOException ex) {
            Logger.getLogger(ConsolidateResults.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void consolidade(String[] ids, String[] partitions, String[] workers) {
        PrintWriter pw = null;

        for (String id : ids) {

            try {

                pw = new PrintWriter(new FileWriter(new File("consolidated" + id + ".txt")));

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            for (String partition : partitions) {
                String line = partition;
                for (String w : workers) {

                    String pathL = "resultsLockFree_" + id + "_" + partition + "_" + w + ".txt";
                    double tpL = loadTP(pathL);
                    line = line + " " + tpL;

                }
                System.out.println(line);

                for (String w : workers) {
                    int nw=Integer.parseInt(w)*Integer.parseInt(partition);
                    String pathH = "resultsHibrid_" + id + "_" + partition + "_" + nw + ".txt";
                    double tpH = loadTP(pathH);
                    line = line + " " + tpH;
                }

                System.out.println(line);
                pw.println(line);

                pw.flush();
            }

            pw.close();
        }
    }
    
    public void consolidade1shard(String[] ids, String[] partitions, String[] workers) {
        PrintWriter pw = null;

        for (String id : ids) {

            try {

                pw = new PrintWriter(new FileWriter(new File("consolidated" + id + ".txt")));

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            pw.println("Threads LockFree Hybrid");
            
            for (String partition : partitions) {
               
                for (String w : workers) {
                    String line = w;
                    String pathL = "resultsEarly_" + id + "_" + partition + "_" + w + ".txt";
                    double tpL = loadTP(pathL);
                    line = line + " " + tpL;

                    
                    /*String pathH = "resultsHibrid_" + id + "_" + partition + "_" + w + ".txt";
                    double tpH = loadTP(pathH);
                    line = line + " " + tpH;*/
                    System.out.println(line);
                    pw.println(line);
                }

               

                pw.flush();
            }

            pw.close();
        }
    }

    private double loadTP(String path) {
        try {

            FileReader fr = new FileReader(path);
            BufferedReader rd = new BufferedReader(fr);
            String line = null;
            //int j = 0;
            LinkedList<Double> l = new LinkedList<Double>();
            int nextSec = 0;
            while (((line = rd.readLine()) != null)) {
                StringTokenizer st = new StringTokenizer(line, " ");
                try {
                    int i = Integer.parseInt(st.nextToken());
                    if (i <= 120) {

                        String t = st.nextToken();
                        //System.out.println(t);

                        double d = Math.abs(Double.parseDouble(t));

                        if (i > nextSec) {

                            //System.out.println("entrou para i = "+i+" e next sec = "+nextSec);
                            for (int z = nextSec; z < i; z++) {
                                l.add(d);

                            }
                            nextSec = i;

                            //System.out.println("saiu com i = "+i+" e next sec = "+nextSec);
                        } else {
                            //System.out.println("nao entrou i = "+i+" e next sec = "+nextSec);
                        }

                        if (i == nextSec) {
                            l.add(d);
                            nextSec++;
                        }
                        //System.out.println("adicionou "+nextSec);
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                }

            }
            fr.close();
            rd.close();

            //System.out.println("Size: " + l.size());
            double sum = 0;
            int i;
            for (i = 0; i < l.size(); i++) {
                sum = sum + l.get(i);
            }

            /* double md1 = sum/250;
            sum = 0;
            for(i = 251; i < l.size(); i++){
                sum = sum + l.get(i);
            }
            double md2 = sum/(l.size()-250);
            
            
            System.out.println("Media: "+((md1+md2)/2));*/
            //System.out.println("Sum: " + sum);
            //System.out.println("Throughput: " + (sum / l.size()));
            return (sum / l.size());
        } catch (Exception e) {
            e.printStackTrace(System.out);
            return -100;
        }
    }
    
    
     private double loadTP1(String path) {
        try {

            FileReader fr = new FileReader(path);
            BufferedReader rd = new BufferedReader(fr);
            String line = rd.readLine();
            StringTokenizer st = new StringTokenizer(line, " ");
            st.nextToken();
            st.nextToken();
               fr.close();
            rd.close();
            return Double.parseDouble(st.nextToken());
                
         

        } catch (Exception e) {
            e.printStackTrace(System.out);
            return 0;
        }
    }
}
