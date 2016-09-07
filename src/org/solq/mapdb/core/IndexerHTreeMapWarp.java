package org.solq.mapdb.core;

import org.solq.mapdb.model.ISerializer;

/**
 * Indexer 索引数据库什么也不干 
 * @author solq
 **/
public class IndexerHTreeMapWarp<K, V> extends AbstractHTreeMapWarp<K, V> {

    public IndexerHTreeMapWarp(String scanPath, String localDb, boolean buildIndex, ISerializer<K> sk, ISerializer<V> sv) {
	super(scanPath, localDb, buildIndex, sk, sv);
    }

    @Override
    public void doPut(K key, V value) {

    }

    @Override
    public void doRemove(java.lang.Object obj) {

    }

    @Override
    public void doClear() {

    }

    @Override
    public void doGet(Object key, V ret) {

    }

}
