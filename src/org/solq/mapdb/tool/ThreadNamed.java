package org.solq.mapdb.tool;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadNamed {

    private static AtomicInteger count = new AtomicInteger(1);

    public static Thread newThread(String name, Runnable r) {
	return new Thread(r, name + ":" + count.getAndIncrement());
    }
}
