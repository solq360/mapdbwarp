package org.solq.test.model;

import java.util.HashMap;
import java.util.Map;

import org.solq.mapdb.model.ID;
import org.solq.mapdb.model.MapDbConfig;

@MapDbConfig(checkPeriod = 500, exTime = 1, maxSize = 3)
public class Player implements ID<Long> {

    private long id;
    private String name;

    private String addr;

    private Map<String, Integer> pack;

    public static Player of(long id, String name, String addr) {
	Player ret = new Player();
	ret.id = id;
	ret.name = name;
	ret.addr = addr;
	ret.pack = new HashMap<>();
	for (int i = 0; i < 200; i++) {
	    ret.pack.put(i + "", i);
	}
	return ret;
    }

    public static Player ofOver(long id, String name, String addr) {
	Player ret = new Player();
	ret.id = id;
	ret.name = name;
	ret.addr = addr;
	return ret;
    }

    public Long getId() {
	return id;
    }

    void setId(long id) {
	this.id = id;
    }

    public String getName() {
	return name;
    }

    void setName(String name) {
	this.name = name;
    }

    public String getAddr() {
	return addr;
    }

    void setAddr(String addr) {
	this.addr = addr;
    }

    public Map<String, Integer> getPack() {
	return pack;
    }

    void setPack(Map<String, Integer> pack) {
	this.pack = pack;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (id ^ (id >>> 32));
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	Player other = (Player) obj;
	if (id != other.id)
	    return false;
	return true;
    }

}
