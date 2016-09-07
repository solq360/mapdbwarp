package org.solq.mapdb.tool;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public abstract class Tool {
    public final static int count = 500000;

    public static void printlnTime(String info, long start) {
	long end = System.currentTimeMillis();
	System.out.println(info + (end - start));
    }

    public static Set<String> listDirectory(String scanPath) {
	Set<String> ret = listDirectory(scanPath, null);
	return ret;
    }

    public static Set<String> listDirectory(String scanPath, Function<File, Boolean> filter) {
	Set<String> ret = new HashSet<>();
	File file = new File(scanPath);
	if (!file.exists()) {
	    file.mkdirs();
	}
	if (file.isDirectory()) {
	    for (File f : file.listFiles()) {
		if (filter != null && !filter.apply(f)) {
		    continue;
		}
		Set<String> tmp = listDirectory(f.getAbsolutePath());
		ret.addAll(tmp);
	    }
	    return ret;
	}

	if (filter != null && !filter.apply(file)) {
	    return ret;
	}

	ret.add(file.getAbsolutePath());
	return ret;
    }

    public static String pathAddFileName(String path, String... names) {
	String ret = path;
	for (String name : names) {
	    ret = _pathAddFileName(ret, name);
	}
	return ret;
    }

    static String _pathAddFileName(String path, String name) {
	int index = path.lastIndexOf('/') + 1;
	if (index == path.length()) {
	    return path + name;
	}
	index = path.lastIndexOf('\\') + 1;
	if (index == path.length()) {
	    return path + name;
	}
	return path + File.separator + name;
    }
}
