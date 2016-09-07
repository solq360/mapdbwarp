package org.solq.mapdb.model;

import org.mapdb.Serializer;

/**
 * 存储编码接口
 * @author solq
 * */
public interface ISerializer<T> {
    public Class<T> getType();
    public Serializer<T> getSerializer();
}
