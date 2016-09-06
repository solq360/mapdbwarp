package org.solq.test.mapdb;

import org.junit.Test;
import org.mapdb.Serializer;
import org.solq.mapdb.core.HTreeMapWarp;
import org.solq.mapdb.core.SerializerJson;
import org.solq.mapdb.model.MapDbConfig;
import org.solq.test.model.Player;

public class TestWarp {

    @Test
    public void test() throws InterruptedException {
	Long key = 1L;
	HTreeMapWarp<Long, Player> hmw = HTreeMapWarp.of("d://a/", "test", Serializer.LONG, SerializerJson.of(Player.class));
	hmw.clear();
	hmw.put(key, Player.of(key, "name1", "add1"));
	key = 2L;
	hmw.put(key, Player.of(key, "name1", "add1"));
	key = 3L;
	hmw.put(key, Player.of(key, "name1", "add1"));
	System.out.println(hmw.size());
	Thread.sleep(50000);
	System.out.println(hmw.cacheSize());
    }

    @Test
    public void getAnno() throws InterruptedException {
	MapDbConfig anno = Player.class.getAnnotation(MapDbConfig.class);
	System.out.println(anno == null);
    }

}
