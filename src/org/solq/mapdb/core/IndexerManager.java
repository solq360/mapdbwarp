package org.solq.mapdb.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.solq.mapdb.anno.IndexConfig;
import org.solq.mapdb.tool.ReflectUtils;

/***
 * 索引管理
 * 
 * @author solq
 */
public abstract class IndexerManager {

    public static final String INDEX_ID = "mapDbIndex";
    private static Map<Class<?>, List<IndexInfo>> indexMap = new HashMap<>();

    public static synchronized List<IndexInfo> register(Class<?> clazz) {
	List<IndexInfo> list = indexMap.getOrDefault(clazz, new LinkedList<>());
	if (!list.isEmpty()) {
	    return new ArrayList<>(list);
	}
	ReflectUtils.forEachClassField(clazz, (f) -> {
	    IndexConfig anno = f.getAnnotation(IndexConfig.class);
	    if (anno == null) {
		return;
	    }
	    list.add(IndexInfo.of(clazz, f, anno));
	});
	return new ArrayList<>(list);
    }

    static class IndexInfo {
	private Class<?> clazz;
	private Field field;
	private IndexConfig anno;
	private String indexName;

	public static IndexInfo of(Class<?> clazz, Field field, IndexConfig anno) {
	    IndexInfo ret = new IndexInfo();
	    ret.field = field;
	    ret.anno = anno;
	    ret.indexName = "".equals(anno.value()) ? field.getName() : anno.value();
	    return ret;
	}

	public static IndexInfo of(String indexName) {
	    IndexInfo ret = new IndexInfo(); 
	    ret.indexName = indexName;
	    return ret;
	}
	
	public boolean readReBuildIndex() {
	    return anno.readReBuildIndex();
	}

	public Object getValue(Object obj) {
	    try {
		return field.get(obj);
	    } catch (IllegalArgumentException | IllegalAccessException e) {
		e.printStackTrace();
	    }
	    return null;
	}

	public Field getField() {
	    return field;
	}

	public IndexConfig getAnno() {
	    return anno;
	}

	public Class<?> getClazz() {
	    return clazz;
	}

	public String getIndexName() {
	    return indexName;
	}

	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
	    return result;
	}

	@Override
	public boolean equals(Object obj) {
	    if (obj == null)
		return false;
	    if (this == obj)
		return true;

	    if (getClass() != obj.getClass())
		return false;
	    IndexInfo other = (IndexInfo) obj;
	    if (indexName == null) {
		if (other.indexName != null)
		    return false;
	    } else if (!indexName.equals(other.indexName))
		return false;
	    return true;
	}



    }
}
