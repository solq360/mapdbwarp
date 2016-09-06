package org.solq.mapdb.core;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializerObjectArray;
import org.solq.mapdb.model.ISerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * json编码
 * 
 * @author solq
 **/
public class SerializerJson<T> extends GroupSerializerObjectArray<T> implements ISerializer<T> {
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Class<T> classType;

    public static <T> Serializer<T> of(Class<T> clz) {
	SerializerJson<T> ret = new SerializerJson<T>();
	ret.classType = clz;
	return ret;
    }

    @Override
    public Class<T> getType() {
	return classType;
    }

    @Override
    public void serialize(DataOutput2 out, Object value) throws IOException {
	byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(value);
	out.writeInt(bytes.length);
	out.write(bytes);
	out.flush();
    }

    @Override
    public T deserialize(DataInput2 in, int available) throws IOException {
	int len = in.readInt();
	byte[] bytes = new byte[len];
	in.readFully(bytes, 0, len);
	return OBJECT_MAPPER.readValue(bytes, classType);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
	if (vals.getClass().isArray()) {
	    Object[] ar = (Object[]) vals;
	    out.writeShort(ar.length);
	    for (Object v : ar) {
		serialize(out, v);
	    }
	} else {
	    throw new RuntimeException("valueArray2Serialize obj is not array");
	}
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
	int len = in.readUnsignedShort();
	Object[] ret = new Object[len];
	for (int i = 0; i < len; i++) {
	    ret[i] = deserialize(in, size);
	}
	return ret;
    }

}
