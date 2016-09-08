package org.solq.mapdb.core;

import java.io.File;
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
import org.mapdb.DBMaker.Maker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.solq.mapdb.anno.MapDbConfig;
import org.solq.mapdb.model.IAction;
import org.solq.mapdb.model.ID;
import org.solq.mapdb.model.IHTreeMapWarp;
import org.solq.mapdb.model.ISerializer;
import org.solq.mapdb.tool.ThreadNamed;
import org.solq.mapdb.tool.Tool;

import kotlin.Unit;
import kotlin.jvm.functions.Function1;

/**
 * HTreeMap 模板 包装支持多个文件数据
 * 
 * @author solq
 **/
public abstract class AbstractHTreeMapWarp<K, V> implements IHTreeMapWarp<K, V> {
    private final static ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(4);
    protected MapDbConfig annoConfig;
    protected Class<V> vClass;
    protected Class<K> kClass;

    protected String scanRootPath;
    protected String localFile;

    private List<DB> dbList;
    private List<HTreeMap<K, V>> mapList;
    private DB cacheDb;
    private HTreeMap<K, V> cache;

    // private HTreeMap<K, V> locks;.

    /////////////////////////////////////
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
		    // System.out.println(e.fillInStackTrace());
		}
	    }
	}
    });

    public AbstractHTreeMapWarp(String scanPath, String localFile, boolean indexDB, ISerializer<K> sk, ISerializer<V> sv) {
	vClass = sv.getType();
	kClass = sk.getType();
	// 扫描文件
	this.scanRootPath = indexDB ? scanPath : Tool.pathAddFileName(scanPath, vClass.getSimpleName());
	this.localFile = Tool.pathAddFileName(this.scanRootPath, localFile);
	Set<String> files = Tool.listDirectory(this.scanRootPath, indexDB ? null : (f) -> {
	    if (f.getAbsolutePath().lastIndexOf(IndexerManager.INDEX_ID) > -1) {
		return false;
	    }
	    return true;
	});
	File check = new File(this.scanRootPath);
	if (!check.exists()) {
	    check.mkdirs();
	}

	files.add(new File(this.localFile).getAbsolutePath());

	// 构建mapdb实例
	final int size = files.size();
	this.dbList = new ArrayList<>(size);
	this.mapList = new ArrayList<>(size);

	for (String f : files) {
	    Maker mb = DBMaker.fileDB(f).checksumHeaderBypass()
		    // .concurrencyScale(4).allocateStartSize(len)
		    // .executorEnable()
		    .fileLockDisable();
	    if (indexDB) {
		mb.fileMmapEnable().fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable();
	    }
	    DB db = mb.make();
	    this.dbList.add(db);
	    HTreeMap<K, V> map = db.hashMap("map", sk.getSerializer(), sv.getSerializer()).counterEnable().createOrOpen();
	    this.mapList.add(map);
	}
	annoConfig = vClass.getAnnotation(MapDbConfig.class);
	if (annoConfig == null) {
	    annoConfig = MapDbConfig.DEFAULE;
	}
	// 索引数据库不用创建缓存
	if (!indexDB) {
	    final int exTime = annoConfig.exTime();
	    final int maxSize = annoConfig.maxSize();
	    final int checkPeriod = annoConfig.checkPeriod();
	    this.cacheDb = DBMaker.memoryDirectDB().make();
	    this.cache = this.cacheDb.hashMap("cache", sk.getSerializer(), sv.getSerializer())
		    //
		    .expireAfterUpdate(exTime, TimeUnit.SECONDS).expireAfterGet(exTime, TimeUnit.SECONDS).expireAfterCreate(exTime, TimeUnit.SECONDS).expireMaxSize(maxSize)
		    .expireExecutorPeriod(checkPeriod)
		    // 容量大小
		    // .expireStoreSize(16 * 1024*1024*1024)
		    .expireExecutor(EXECUTOR_SERVICE).create();
	}
	if (annoConfig.openAsync()) {
	    this.asyncThread.setDaemon(true);
	    this.asyncThread.start();
	}
    }

    public abstract void doPut(K key, V value);

    public abstract void doRemove(Object obj);

    public abstract void doGet(Object key, V ret);

    public abstract void doClear();

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
	    if (cache != null) {
		cache.put(key, value);
	    }
	    doPut(key, value);
	}
	return ret;
    }

    @Override
    public V replace(K key, V value) {
	// HTreeMap<K, V> map = findDisk(key);
	// if (map != null) {
	// V ret = map.replace(key, value);
	// if (ret != null) {
	// cache.put(key, value);
	// return ret;
	// }
	// }
	// return null;
	throw new RuntimeException("不实现 replace(K key, V value) ");
    }

    @Override
    public boolean remove(Object key, Object value) {
	// for (HTreeMap<K, V> map : mapList) {
	// if (map.remove(key, value)) {
	// cache.remove(key);
	// return true;
	// }
	// }
	// return false;
	throw new RuntimeException("不实现 remove (Object key, Object value) ");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
	// HTreeMap<K, V> map = findDisk(key);
	// if (map != null) {
	// if (map.replace(key, oldValue, newValue)) {
	// cache.put(key, newValue);
	// return true;
	// }
	// }
	// return false;
	throw new RuntimeException("不实现 replace(K key, V oldValue, V newValue) ");
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
	if (cache != null && !cache.isEmpty()) {
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
	if (cache != null && cache.containsKey(key)) {
	    return false;
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
	if (cache != null && cache.containsValue(value)) {
	    return false;
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
	if (!kClass.isInstance(key)) {
	    return null;
	}
	V ret = cache != null ? cache.get(key) : null;
	if (ret == null) {
	    HTreeMap<K, V> map = findDisk(key);
	    if (map != null) {
		ret = map.get(key);
	    }
	}
	doGet(key, ret);
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
	if (cache != null) {
	    cache.put(key, value);
	}
	IAction action = () -> {
	    HTreeMap<K, V> map = findDisk(key);
	    if (map == null) {
		map = mapList.get(0);
	    }
	    map.put(key, value);
	    doPut(key, value);
	};
	if (annoConfig.openAsync()) {
	    // 不阻塞提交
	    queue.add(action);
	} else {
	    action.run();
	}

	return value;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public V remove(Object obj) {
	Object key = obj instanceof ID ? ((ID) obj).getId() : obj;
	IAction action = () -> {
	    for (HTreeMap<K, V> map : mapList) {
		V ret = map.remove(key);
		if (ret != null) {
		    doRemove(key);
		    break;
		}
	    }
	};
	if (annoConfig.openAsync()) {
	    // 不阻塞提交
	    queue.add(action);
	} else {
	    action.run();
	}
	if (cache == null) {
	    return null;
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
	if (cache != null) {
	    cache.clear();
	}
	for (HTreeMap<K, V> map : mapList) {
	    map.clear();
	}
	doClear();
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
	if (cache != null) {
	    cache.close();
	}
	if (cacheDb != null) {
	    cacheDb.close();
	}
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
	return cache != null ? cache.size() : 0;
    }

}
