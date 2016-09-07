package org.solq.mapdb.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.solq.mapdb.core.IndexerManager.IndexInfo;
import org.solq.mapdb.model.IIdexer;
import org.solq.mapdb.model.ISerializer;
import org.solq.mapdb.tool.Tool;

import kotlin.jvm.functions.Function2;

/**
 * HTreeMap 包装支持多个文件数据
 * 
 * @author solq
 **/
public class HTreeMapWarp<K, V> extends AbstractHTreeMapWarp<K, V> implements IIdexer {

    private Map<IndexInfo, IndexerHTreeMapWarp<K, Object>> indexers;

    public static <K, V> HTreeMapWarp<K, V> of(String scanPath, String localDb, boolean buildIndex, ISerializer<K> sk, ISerializer<V> sv) {
	HTreeMapWarp<K, V> ret = new HTreeMapWarp<K, V>(scanPath, localDb, buildIndex, sk, sv);
	return ret;
    }

    @SuppressWarnings("unchecked")
    public HTreeMapWarp(String scanPath, String localDb, boolean buildIndex, ISerializer<K> sk, ISerializer<V> sv) {
	super(scanPath, localDb, buildIndex, sk, sv);
	// 生成索引
	List<IndexInfo> indexInfoList = IndexerManager.register(vClass);
	indexers = new HashMap<>(indexInfoList.size());
	indexInfoList.forEach((e) -> {
	    // rootpath+classname+index_id+indexName+childfile
	    String scanIndexPath = Tool.pathAddFileName(scanPath, vClass.getSimpleName(), IndexerManager.INDEX_ID, e.getField().getName());
	    ISerializer<Object> isv = (ISerializer<Object>) SerializerProxy.ofSerializerJson(e.getField().getType());
	    indexers.put(e, new IndexerHTreeMapWarp<>(scanIndexPath, localDb, true, sk, isv));
	});
    }

    @SuppressWarnings({ "unchecked", "hiding" })
    @Override
    public <K, V, IV> void forEach(String indexName, Function2<K, IV, Boolean> filter, Consumer<? super V> action) {
	IndexerHTreeMapWarp<K, IV> map = (IndexerHTreeMapWarp<K, IV>) indexers.get(IndexInfo.of(indexName));
	if (map == null) {
	    return;
	}
	BiConsumer<K, IV> proxyCb = (K t, IV u) -> {
	    if (!filter.invoke(t, u)) {
		return;
	    }
	    V value = (V) HTreeMapWarp.this.get(t);
	    if (value == null) {
		// 是否删除索引数据
		return;
	    }
	    action.accept(value);
	};
	map.forEach(proxyCb);
    }

    @Override
    public void doRemove(Object obj) {
	for (Entry<IndexInfo, IndexerHTreeMapWarp<K, Object>> entry : indexers.entrySet()) {
	    entry.getValue().remove(obj);
	}
    }

    @Override
    public void doPut(K key, V value) {
	for (Entry<IndexInfo, IndexerHTreeMapWarp<K, Object>> entry : indexers.entrySet()) {
	    IndexerHTreeMapWarp<K, Object> map = entry.getValue();
	    Object v = entry.getKey().getValue(value);
	    map.put(key, v);
	}
    }

    @SuppressWarnings("unchecked")
    @Override
    public void doGet(Object key, V value) {
	if (value == null) {
	    return;
	}
	for (Entry<IndexInfo, IndexerHTreeMapWarp<K, Object>> entry : indexers.entrySet()) {
	    IndexInfo info = entry.getKey();
	    if (info.readReBuildIndex()) {
		IndexerHTreeMapWarp<K, Object> map = entry.getValue();
		Object v = info.getValue(value);
		map.put((K) key, v);
	    }
	}
    }

    @Override
    public void doClear() {
	for (IndexerHTreeMapWarp<K, ?> indexer : indexers.values()) {
	    indexer.clear();
	}
    }

    @Override
    public void close() throws IOException {
	super.close();
	for (IndexerHTreeMapWarp<K, ?> indexer : indexers.values()) {
	    indexer.close();
	}
    }

}
