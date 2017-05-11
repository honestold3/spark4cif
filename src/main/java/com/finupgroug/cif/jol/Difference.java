package com.finupgroug.cif.jol;

import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.vm.VM;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.out;

/**
 * Created by wq on 2017/1/13.
 */
public class Difference {

    public static void main(String[] args) throws Exception {
        out.println(VM.current().details());

        Map<String, String> chm = new ConcurrentHashMap<String, String>();

        GraphLayout gl1 = GraphLayout.parseInstance(chm);

        chm.put("Foo", "Bar");
        GraphLayout gl2 = GraphLayout.parseInstance(chm);

        chm.put("Foo2", "Bar2");
        GraphLayout gl3 = GraphLayout.parseInstance(chm);

        out.println("size:"+chm.size());

        System.out.println(gl2.subtract(gl1).toPrintable());
        System.out.println(gl3.subtract(gl2).toPrintable());
        System.out.println("total:"+gl1.totalSize());

    }
}