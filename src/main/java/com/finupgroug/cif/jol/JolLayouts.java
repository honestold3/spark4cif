package com.finupgroug.cif.jol;

import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.vm.VM;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.out;

/**
 * Created by wq on 2017/1/13.
 */
public class JolLayouts {

    public static void main(String[] args) throws Exception {
        out.println(VM.current().details());

        PrintWriter pw = new PrintWriter(System.out, true);

        Map<Dummy, Void> map = new HashMap<Dummy, Void>();

        map.put(new Dummy(1), null);
        map.put(new Dummy(2), null);

        System.gc();
        pw.println(GraphLayout.parseInstance(map).toPrintable());

        map.put(new Dummy(2), null);
        map.put(new Dummy(2), null);
        map.put(new Dummy(2), null);
        map.put(new Dummy(2), null);

        System.gc();
        pw.println(GraphLayout.parseInstance(map).toPrintable());

        for (int c = 0; c < 12; c++) {
            map.put(new Dummy(2), null);
        }

        System.gc();
        pw.println(GraphLayout.parseInstance(map).toPrintable());

        pw.close();
    }

    /**
     * Dummy class which controls the hashcode and is decently Comparable.
     */
    public static class Dummy implements Comparable<Dummy> {
        static int ID;
        final int id = ID++;
        final int hc;

        public Dummy(int hc) {
            this.hc = hc;
        }

        @Override
        public boolean equals(Object o) {
            return (this == o);
        }

        @Override
        public int hashCode() {
            return hc;
        }

        @Override
        public int compareTo(Dummy o) {
            return (id < o.id) ? -1 : ((id == o.id) ? 0 : 1);
        }
    }

}
