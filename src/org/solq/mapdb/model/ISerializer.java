package org.solq.mapdb.model;

public interface ISerializer<T> {
    public Class<T> getType();
}
