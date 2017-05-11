package com.finupgroug.cif.jol;

import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.out;


/**
 * Created by wq on 2017/1/13.
 */
public class JOLDemo {

    public static void main(String[] args) throws Exception {

        out.println(VM.current().details());
        out.println(ClassLayout.parseClass("asdf".getClass()).toPrintable());
        out.println(ClassLayout.parseInstance("asdf").toPrintable());

        out.println("Graph:"+GraphLayout.parseInstance("asdf").toPrintable());
        out.println("total:"+GraphLayout.parseInstance("asdf").totalSize());

        System.out.println("--------------------------");
        ArrayList<Integer> al = new ArrayList<Integer>();
        LinkedList<Integer> ll = new LinkedList<Integer>();

        for (int i = 0; i < 1000; i++) {
            Integer io = i; // box once
            al.add(io);
            ll.add(io);
        }

        al.trimToSize();
        out.println(ClassLayout.parseInstance(al).toPrintable());

//        PrintWriter pw = new PrintWriter(out);
//        pw.println(GraphLayout.parseInstance(al).toFootprint());
//        pw.println(GraphLayout.parseInstance(ll).toFootprint());
//        pw.println(GraphLayout.parseInstance(al, ll).toFootprint());
//        pw.close();
    }

    public static class A {
        boolean f;
    }

}
