package org.solq.mapdb.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.solq.mapdb.model.IAction;
import org.solq.mapdb.model.ID;
import org.solq.mapdb.model.IHTreeMapWarp;
import org.solq.mapdb.model.ISerializer;
import org.solq.mapdb.model.MapDbConfig;
import org.solq.mapdb.tool.ThreadNamed;
import org.solq.mapdb.tool.Tool;

import kotlin.Unit;
import kotlin.jvm.functions.Function1;

/**
 * HTreeMap 包装支持多个文件数据
 * 
 * @author solq
 **/
public class HTreeMapWarp<K, V extends ID<K>> implements IHTreeMapWarp<K, V> {
    private final static ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    private List<DB> dbList;
    private List<HTreeMap<K, V>> mapList;
    private DB cacheDb;
    private HTreeMap<K, V> cache;
    // private HTreeMap<K, V> locks;
    /////////////////////////////////////
    private boolean openAsync = true;
    private LinkedBlockingQueue<IAction> queue = new LinkedBlockingQueue<>();
    private Thread asyncThread = ThreadNamed.newThread("异步回写 mapdb数据", new Runnable() {

	@Override
	public void run() {
	    while (true) {
		try {
		    IAction action = queue.take();
		    action.run();
		} catch (InterruptedException e) {
		    e.fillInStackTrace();
		    break;
		} catch (Exception e) {
		    e.fillInStackTrace();
		}
	    }
	}
    });

    @SuppressWarnings({ "unchecked" })
    public static <K, V extends ID<K>> HTreeMapWarp<K, V> of(String scanPath, String localDb, Serializer<K> sk, Serializer<V> sv) {

	Set<String> files = Tool.listDirectory(scanPath);
	files.add(localDb);

	final int size = files.size();
	HTreeMapWarp<K, V> ret = new HTreeMapWarp<K, V>();
	ret.dbList = new ArrayList<>(size);
	ret.mapList = new ArrayList<>(size);

	for (String f : files) {
	    DB db = DBMaker.fileDB(f).checksumHeaderBypass()
		    // .concurrencyScale(4).allocateStartSize(len)
		    // .executorEnable()
		    .fileLockDisable()
		    // .fileMmapEnable().fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable()
		    .make();
	    ret.dbList.add(db);
	    HTreeMap<K, V> map = db.hashMap("map", sk, sv).counterEnable().createOrOpen();
	    ret.mapList.add(map);
	}
	MapDbConfig annoConfig = null;
	if (sv instanceof ISerializer) {
	    Class<V> vClass = ((ISerializer<V>) sv).getType();
	    annoConfig = vClass.getAnnotation(MapDbConfig.class);
	}
	if (annoConfig == null) {
	    annoConfig = MapDbConfig.DEFAULE;
	}
	int exTime = annoConfig.exTime();
	int maxSize = annoConfig.maxSize();
	int checkPeriod = annoConfig.checkPeriod();
	ret.cacheDb = DBMaker.memoryDirectDB().make();
	ret.cache = ret.cacheDb.hashMap("cache", sk, sv)
		//
		.expireAfterUpdate(exTime, TimeUnit.SECONDS).expireAfterGet(exTime, TimeUnit.SECONDS).expireAfterCreate(exTime, TimeUnit.SECONDS).expireMaxSize(maxSize)
		.expireExecutorPeriod(checkPeriod)
		// 容量大小
		// .expireStoreSize(16 * 1024*1024*1024)
		.expireExecutor(EXECUTOR_SERVICE).create();
	ret.asyncThread.setDaemon(true);
	ret.asyncThread.start();
	return ret;
    }

    // 缺少主键加入，返回NULL ，否则不允许插入，返回上次成功数据
    @Override
    public V putIfAbsent(K key, V value) {
	HTreeMap<K, V> map = findDisk(key);
	V ret = null;
	if (map == null) {
	    mapList.get(0).put(key, value);
	} else {
	    ret = map.putIfAbsent(key, value);
	}
	if (ret == null) {
	    cache.put(key, value);
	}
	return ret;
    }

    @Override
    public boolean remove(Object key, Object value) {
	for (HTreeMap<K, V> map : mapList) {
	    if (map.remove(key, value)) {
		cache.remove(key);
		return true;
	    }
	}
	return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
	HTreeMap<K, V> map = findDisk(key);
	if (map != null) {
	    if (map.replace(key, oldValue, newValue)) {
		cache.put(key, newValue);
		return true;
	    }
	}
	return false;
    }

    @Override
    public V replace(K key, V value) {
	HTreeMap<K, V> map = findDisk(key);
	if (map != null) {
	    V ret = map.replace(key, value);
	    if (ret != null) {
		cache.put(key, value);
		return ret;
	    }
	}
	return null;
    }

    @Override
    public int size() {
	int ret = 0;
	for (HTreeMap<K, V> map : mapList) {
	    ret += map.size();
	}
	return ret;
    }

    @Override
    public boolean isEmpty() {
	if (!cache.isEmpty()) {
	    return false;
	}
	for (HTreeMap<K, V> map : mapList) {
	    if (!map.isEmpty()) {
		return false;
	    }
	}
	return true;
    }

    @Override
    public boolean containsKey(Object key) {
	if (cache.containsKey(key)) {
	    return true;
	}
	for (HTreeMap<K, V> map : mapList) {
	    if (map.containsKey(key)) {
		return true;
	    }
	}
	return false;
    }

    @Override
    public boolean containsValue(Object value) {
	if (cache.containsValue(value)) {
	    return true;
	}
	for (HTreeMap<K, V> map : mapList) {
	    if (map.containsValue(value)) {
		return true;
	    }
	}
	return false;
    }

    @Override
    public V get(Object key) {
	V ret = cache.get(key);
	if (ret != null) {
	    return ret;
	}
	HTreeMap<K, V> map = findDisk(key);
	if (map != null) {
	    ret = map.get(key);
	}
	return ret;
    }

    @Override
    public HTreeMap<K, V> findDisk(Object key) {
	for (HTreeMap<K, V> map : mapList) {
	    if (map.containsKey(key)) {
		return map;
	    }
	}
	return null;
    }

    @Override
    public V put(K key, V value) {
	cache.put(key, value);
	IAction action = () -> {
	    HTreeMap<K, V> map = findDisk(key);
	    if (map == null) {
		map = mapList.get(0);
	    }
	    map.put(key, value);
	};
	if (openAsync) {
	    // 不阻塞提交
	    queue.add(action);
	} else {
	    action.run();
	}

	return value;
    }

    @Override
    public V remove(Object key) {

	IAction action = () -> {
	    for (HTreeMap<K, V> map : mapList) {
		V ret = map.remove(key);
		if (ret != null) {
		    break;
		}
	    }
	};
	if (openAsync) {
	    // 不阻塞提交
	    queue.add(action);
	} else {
	    action.run();
	}
	return cache.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
	for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
	    put(entry.getKey(), entry.getValue());
	}
    }

    @Override
    public void clear() {
	cache.clear();
	for (HTreeMap<K, V> map : mapList) {
	    map.clear();
	}
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<K> keySet() {
	Set<K> ret = new HashSet<>(size());
	for (HTreeMap<K, V> map : mapList) {
	    ret.addAll(map.keySet());
	}
	return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> values() {
	Collection<V> ret = new ArrayList<>(size());
	for (HTreeMap<K, V> map : mapList) {
	    ret.addAll(map.values());
	}
	return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
	Set<Map.Entry<K, V>> ret = new HashSet<>(size());
	for (HTreeMap<K, V> map : mapList) {
	    ret.addAll(map.entrySet());
	}
	return ret;
    }

    @Override
    public void close() throws IOException {
	for (HTreeMap<K, V> map : mapList) {
	    map.close();
	}
	for (DB db : dbList) {
	    db.close();
	}
	cache.close();
	cacheDb.close();
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> arg0) {
	for (HTreeMap<K, V> map : mapList) {
	    map.forEach(arg0);
	}
    }

    @Override
    public void forEachKey(Function1<? super K, Unit> arg0) {
	for (HTreeMap<K, V> map : mapList) {
	    map.forEachKey(arg0);
	}
    }

    @Override
    public void forEachValue(Function1<? super V, Unit> arg0) {
	for (HTreeMap<K, V> map : mapList) {
	    map.forEachValue(arg0);
	}
    }

    @Override
    public Serializer<K> getKeySerializer() {
	return mapList.get(0).getKeySerializer();
    }

    @Override
    public Serializer<V> getValueSerializer() {
	return mapList.get(0).getValueSerializer();
    }

    @Override
    public boolean putIfAbsentBoolean(K k, V v) {
	throw new RuntimeException("不实现 putIfAbsentBoolean 方法");
    }

    @Override
    public boolean isClosed() {
	for (HTreeMap<K, V> map : mapList) {
	    if (!map.isClosed()) {
		return false;
	    }
	}
	return true;
    }

    @Override
    public long sizeLong() {
	long ret = 0;
	for (HTreeMap<K, V> map : mapList) {
	    ret += map.sizeLong();
	}
	return ret;
    }

    @Override
    public int cacheSize() {
	return cache.size();
    }

}
