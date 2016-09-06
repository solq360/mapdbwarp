package org.solq.mapdb.tool;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public abstract class Tool {
    public final static int count = 500000;

    public static void printlnTime(String info, long start) {
 	long end = System.currentTimeMillis();
 	System.out.println(info + (end - start));
     }
    

    public static Set<String> listDirectory(String scanPath) {
	Set<String> ret = new HashSet<>();
	File file = new File(scanPath);
	if (!file.exists()) {
	    file.mkdirs();
	}
	if (file.isDirectory()) {
	    for (File f : file.listFiles()) {
		Set<String> tmp = listDirectory(f.getAbsolutePath());
		ret.addAll(tmp);
	    }
	} else {
	    ret.add(scanPath);
	}
	return ret;
    }
}
