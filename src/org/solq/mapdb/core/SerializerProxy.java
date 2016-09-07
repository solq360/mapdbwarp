package org.solq.mapdb.core;

import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.solq.mapdb.model.ISerializer;

/**
 * json编码
 * 
 * @author solq
 **/
public class SerializerProxy<T> implements ISerializer<T> {
    private Class<T> type;
    private Serializer<T> serializer;

    public static <T> SerializerProxy<T> of(Class<T> clz, GroupSerializer<T> serializer) {
	SerializerProxy<T> ret = new SerializerProxy<T>();
	ret.type = clz;
	ret.serializer = serializer;
	return ret;
    }

    public static <T> ISerializer<T> ofSerializerJson(Class<T> clz) {
	SerializerProxy<T> ret = new SerializerProxy<T>();
	ret.type = clz;
	ret.serializer = SerializerJson.of(clz);
	return ret;
    }

    @Override
    public Class<T> getType() {
	return type;
    }

    @Override
    public Serializer<T> getSerializer() {
	return serializer;
    }

}
