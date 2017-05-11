package com.finupgroug.cif.jol;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import java.io.PrintWriter;

import static java.lang.System.out;

/**
 * Created by wq on 2017/1/13.
 */
public class Promotion {

    static volatile Object sink;

    public static void main(String[] args) throws Exception {
        out.println(VM.current().details());

        PrintWriter pw = new PrintWriter(System.out, true);

        Object o = new Object();

        ClassLayout layout = ClassLayout.parseInstance(o);

        long lastAddr = VM.current().addressOf(o);
        pw.printf("Fresh object is at %x%n", lastAddr);

        int moves = 0;
        for (int i = 0; i < 100000; i++) {
            long cur = VM.current().addressOf(o);
            if (cur != lastAddr) {
                moves++;
                pw.printf("*** Move %2d, object is at %x%n", moves, cur);
                out.println(layout.toPrintable());
                lastAddr = cur;
            }

            // make garbage
            for (int c = 0; c < 10000; c++) {
                sink = new Object();
            }
        }

        pw.close();
    }

}
