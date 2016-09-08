package org.solq.test.mapdb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.mapdb.Serializer;
import org.solq.mapdb.anno.MapDbConfig;
import org.solq.mapdb.core.HTreeMapWarp;
import org.solq.mapdb.core.SerializerProxy;
import org.solq.mapdb.tool.Tool;
import org.solq.test.model.Player;

public class TestWarp {

    @Test
    public void test() throws InterruptedException {
	Long key = 1L;
	HTreeMapWarp<Long, Player> hmw = HTreeMapWarp.of("d://a/", "test", false, SerializerProxy.of(Long.class, Serializer.LONG), SerializerProxy.ofSerializerJson(Player.class));
	hmw.clear();
	hmw.put(key, Player.of(key, "name1", "add1"));
	key = 2L;
	hmw.put(key, Player.of(key, "name1", "add1"));
	key = 3L;
	Player p = Player.of(key, "name1", "add1");
	hmw.put(key, p);
	System.out.println(hmw.size());
	System.out.println(hmw.cacheSize());
	hmw.remove(p);
	System.out.println(hmw.size());

	hmw.forEach(Player.INDEX_NAME, (Long id, String name) -> {
	    return name.equals("name1");
	} , (Player ep) -> {
	    System.out.println("查询到  : " + ep.getId());
	});
    }

    @Test
    public void pressure_test() throws InterruptedException {
	HTreeMapWarp<Long, Player> hmw = HTreeMapWarp.of("d://a/", "test", false, SerializerProxy.of(Long.class, Serializer.LONG), SerializerProxy.ofSerializerJson(Player.class));
	hmw.clear();
	Long key = 100000L;
	long start = System.currentTimeMillis();
	while (key-- > 0) {
	    hmw.put(key, Player.of(key, "name1", "add1"));
	}
	Tool.printlnTime("写完数据 :", start);
	start = System.currentTimeMillis();

	System.out.println(hmw.size());
	Tool.printlnTime("查询数据总数 :", start);
	start = System.currentTimeMillis();

	hmw.forEach(Player.INDEX_NAME, (Long id, String name) -> {
	    return id > 99990l;
	} , (Player ep) -> {
	    System.out.println("查询到  : " + ep.getId());
	});
	Tool.printlnTime("查询索引 :", start);
	start = System.currentTimeMillis();
    }
    
    @Test
    public void index_test() throws InterruptedException {
	HTreeMapWarp<Long, Player> hmw = HTreeMapWarp.of("d://a/", "test", false, SerializerProxy.of(Long.class, Serializer.LONG), SerializerProxy.ofSerializerJson(Player.class));
	List<Player> ret =new LinkedList<>();
	long start = System.currentTimeMillis(); 
	hmw.forEach(Player.INDEX_NAME, (Long id, String name) -> {
	    return id > 89990l;
	} , (Player ep) -> {
	    ret.add(ep);
	    //System.out.println("查询到  : " + ep.getId());	    
	});
	System.out.println(ret.size());
	Tool.printlnTime("查询索引 :", start);
	start = System.currentTimeMillis();
    }

    @Test
    public void getAnno() throws InterruptedException {
	MapDbConfig anno = Player.class.getAnnotation(MapDbConfig.class);
	System.out.println(anno == null);
    }

}
