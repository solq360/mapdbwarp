package org.solq.mapdb.model;

import java.util.function.Consumer;

import kotlin.jvm.functions.Function2;

/**
 * 索引器接口
 * 
 * @author solq
 */
public interface IIdexer {
    /***
     * 遍历索引从索引里查询并返回数据
     * 
     * @param indexName
     *            索引名
     * @param filter
     *            过滤回调
     * @param action
     *            业务逻辑回调
     */
    public <K, V, IV> void forEach(String indexName, Function2<K, IV, Boolean> filter, Consumer<? super V> action);
}
