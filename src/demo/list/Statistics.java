/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.list;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.StringTokenizer;

/**
 *
 * @author eduardo
 */
public class Statistics {

    public static void main(String[] args) {

        /*File f = new File(args[0]);
        if (!f.exists()) {
            System.exit(0);
        }*/
        //load(args[0]);
        short s1 = 21020;
        short s2 = 1992;

        byte[] returnByteArray = new byte[4];
       // returnByteArray[0] = (byte) (s1 & 0xff);
       // returnByteArray[1] = (byte) ((s1 >>> 8) & 0xff);

        returnByteArray[0] = (byte) (s1 >> 8);
        returnByteArray[1] = (byte) (s1);
       
        
        returnByteArray[2] = (byte) (s2 >> 8);
        returnByteArray[3] = (byte) (s2);
        
       /* returnByteArray[2] = (byte) (s2 & 0xff);
        returnByteArray[3] = (byte) ((s2 >>> 8) & 0xff);*/
      
        ByteBuffer bb = ByteBuffer.wrap(returnByteArray);
        short s3 = bb.getShort();
        short s4 = bb.getShort();
      
        System.out.println("S3: "+s3);
        System.out.println("S4: "+s4);
        
    }

    public static void load(String path) {
        //System.out.println("Vai ler!!!");
        try {

            FileReader fr = new FileReader(path);

            BufferedReader rd = new BufferedReader(fr);
            String line = null;
            int j = 0;
            LinkedList<Double> l = new LinkedList<Double>();
            int nextSec = 0;
            while (((line = rd.readLine()) != null)) {
                StringTokenizer st = new StringTokenizer(line, " ");
                try {
                    int i = Integer.parseInt(st.nextToken());
                    if (i <= 120) {

                        String t = st.nextToken();
                        //System.out.println(t);

                        double d = Double.parseDouble(t);

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
            System.out.println("Throughput: " + (sum / l.size()));
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

}
