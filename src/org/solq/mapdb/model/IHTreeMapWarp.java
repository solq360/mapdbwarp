package org.solq.mapdb.model;

import java.io.Closeable;

import org.mapdb.HTreeMap;
import org.mapdb.MapExtra;

public interface IHTreeMapWarp<K, V extends ID<K>> extends MapExtra<K, V>, Closeable {

    public HTreeMap<K, V> findDisk(Object key);

    public int cacheSize();
}
