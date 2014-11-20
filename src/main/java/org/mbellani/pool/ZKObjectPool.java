package org.mbellani.pool;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static org.mbellani.utils.Net.getAddress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.mbellani.zk.ZKClient;
import org.mbellani.zk.ZKClient.SynchronizedOperationCallback;
import org.mbellani.zk.ZKClient.ZKTransWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

public class ZKObjectPool<T> implements ObjectPool<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZKObjectPool.class);

	private Config config;
	private ZKClient zk;
	private PoolPaths paths;
	private Map<T, String> borrowed = new HashMap<T, String>();
	private ObjectFactory<T> factory;
	private boolean shutdown;
	private String id;
	private MetricRegistry metrics = new MetricRegistry();
	private Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
	        .outputTo(LoggerFactory.getLogger("org.mbellani.pool-perf")).build();
	private TaskManager<T> taskManager;
	private Ordering<String> nodeSorter = new Ordering<String>() {
		public int compare(String leftNode, String rightNode) {
			return Ints.compare(Integer.parseInt(leftNode), Integer.parseInt(rightNode));
		}
	};

	public ZKObjectPool(Config config) {
		checkArgument(config != null, "Please provide a valid zookeeper configuration.");
		config.validate();
		this.config = config;
	}

	public ObjectFactory<T> getFactory() {
		return factory;
	}

	public void setFactory(ObjectFactory<T> factory) {
		this.factory = factory;
	}

	public ZKClient getZk() {
		return zk;
	}

	public Config getConfig() {
		return config;
	}

	public PoolPaths getPaths() {
		return paths;
	}

	public int getUnused() {
		return zk.getStat(paths.unused()).getNumChildren();
	}

	public int getUsed() {
		return zk.getStat(paths.used()).getNumChildren();
	}

	@Override
	public int getZombies() {
		return zk.getStat(paths.zombies()).getNumChildren();
	}

	public void initialize() {
		startMetricReporter();
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "Initialization")).time();
		try {
			zk = new ZKClient(config.getZkConnectString());
			if (constructPaths()) {
				LOGGER.info("Successfully constructed storage paths, Filling pool to its initial capacity");
				fill();
			}
			registerParticipant();
			zk.sync(paths.used(), new Object());
			taskManager = new TaskManager<T>(this);
			taskManager.start();
		} catch (Exception e) {
			LOGGER.error("Error while initializing the pool ", e);
		} finally {
			ctx.stop();
		}
	}

	public T borrow() {
		if (isFull() || shutdown) {
			return null;
		}
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "borrow")).time();
		T obj = null;
		String node = null;
		registerParticipant();
		try {
			for (;;) {
				node = find();
				obj = markBorrowed(node);
				if (isValid(obj)) {
					break;
				} else {
					invalidate(obj);
				}
			}
		} catch (ZombieException e) {
			handleZombie(node);
			obj = borrow();
		} finally {
			ctx.stop();
		}
		return obj;
	}

	public void returnObject(T object) {
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "return")).time();
		try {
			String node = borrowed.remove(object);
			if (node != null) {
				markUnused(node);
			} else {
				LOGGER.error("No node found to return object {} ", object);
			}
		} finally {
			ctx.stop();
		}
	}

	public boolean invalidate(T object) {
		String node = borrowed.get(object);
		boolean destroyed = false;
		if (node != null) {
			try {
				destroyed = destroyObj(node, object);
				if (destroyed) {
					zk.inTransaction().delete(paths.master().concat("/").concat(node))
					        .delete(paths.used().concat("/").concat(node)).commit();
				}
			} catch (Exception e) {
				LOGGER.error("Error destroying the object {} due to {}", object, e);
			}
		}
		return destroyed;
	}

	public int getSize() {
		return zk.getStat(paths.master()).getNumChildren();
	}

	public void shutdown() {
		if (shutdown) {
			return;
		}
		shutdown = true;
		try {
			LOGGER.info("Shutting down pool.");
			zk.doSynchronized(paths.shutdownLock(), new SynchronizedOperationCallback<Integer>() {
				public Integer perform() throws InterruptedException, KeeperException {
					taskManager.shutdown();
					LOGGER.info("Dregistering participant.");
					deregister();
					int participantsLeft = getParticipants().size();
					destroyAllObjects(participantsLeft);
					return participantsLeft;
				}

			});
			zk.shutdown();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	public List<String> getParticipants() {
		List<String> pList = zk.getChildren(paths.participants());
		List<String> pData = newArrayList();
		if (pList != null && !pList.isEmpty()) {
			for (String p : pList) {
				byte[] data = zk.getData(paths.participants().concat("/" + p));
				String converted = data == null ? "address-unknown" : new String(data);
				pData.add(converted);
			}
		}
		return pData;
	}

	protected void drop(String node) {
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "drp")).time();
		try {
			if (node != null) {
				zk.inTransaction().delete(paths.zombies().concat("/").concat(node))
				        .delete(paths.master().concat("/").concat(node)).delete(paths.used().concat("/").concat(node))
				        .commit();
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			ctx.stop();
		}
	}

	protected boolean isValid(T obj) {
		boolean valid = false;
		try {
			valid = obj != null && factory.validate(obj);
		} catch (ZombieException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.debug("error validating obj {} , destroying", obj);
			invalidate(obj);
		}
		return valid;
	}

	protected T borrowSpecific(String node) {
		T obj = null;
		try {
			if (markUsed(node)) {
				obj = markBorrowed(node);
				if (!isValid(obj)) {
					invalidate(obj);
					obj = null;
				}
			}
		} catch (ZombieException e) {
			obj = null;
			handleZombie(node);
		}
		return obj;
	}

	protected void unzombie(String node) {
		try {
			String zombie_node = paths.zombies().concat("/").concat(node);
			if (zk.exists(zombie_node)) {
				zk.inTransaction().delete(paths.used().concat("/").concat(node)).delete(zombie_node)
				        .create(paths.unused().concat("/").concat(node)).commit();
			}
		} catch (KeeperException.NoNodeException e) {
			// It's ok, node may not be a zombie.
		} catch (InterruptedException e) {
		} catch (KeeperException e) {
			Throwables.propagate(e);
		}
	}

	protected T getData(String node) {
		if (node == null) {
			return null;
		}
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "retrieve-data")).time();
		try {
			byte[] data = zk.getData(paths.master().concat("/" + node));
			T desirializedObj = factory.deserialize(data);
			return desirializedObj;
		} finally {
			ctx.stop();
		}
	}

	protected List<String> getZombieNodes() {
		return zk.getChildren(paths.zombies());
	}

	private void startMetricReporter() {
		if (config.reportMetrics()) {
			reporter.start(config.getMetricIntrvl(), config.getIntrvlUnit());
		}
	}

	private synchronized void registerParticipant() {
		try {
			if (!isRegistered()) {
				String address = getAddress();
				String path = zk.createEphemeralSeq(paths.participants().concat("/"), address.getBytes());
				id = path.substring(path.lastIndexOf("/") + 1);
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	private boolean isRegistered() {
		return id != null && (zk.exists(paths.participants().concat("/").concat(id)));
	}

	private void fill() {
		for (int i = 0; i < config.getInitSize(); i++) {
			addNew(paths.unused());
		}
	}

	private void handleZombie(String node) {
		try {
			zk.create(paths.zombies().concat("/").concat(node));
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	private boolean isFull() {
		return config.getSize() <= getUsed();
	}

	private T markBorrowed(String node) {
		T obj = null;
		if (node != null) {
			obj = getData(node);
			borrowed.put(obj, node);
		}
		return obj;
	}

	private boolean constructPaths() {
		boolean success = false;
		try {
			paths = new PoolPaths(config.getName());
			ZKTransWrapper t = zk.inTransaction();
			for (String path : paths.all()) {
				t.create(path);
			}
			t.commit();
			success = true;
		} catch (KeeperException.NodeExistsException e) {
			// normal, another client has already initialized the pool.
		} catch (Exception e) {
			Throwables.propagate(e);
		}
		return success;
	}

	private String addNew(String toPath) {
		String dataNode = null;
		try {
			// Taking a bit of risk here by not using transaction.
			String path = zk.createSeq(paths.master().concat("/"), createData());
			dataNode = path.substring(path.lastIndexOf("/") + 1);
			zk.create(toPath.concat("/").concat(dataNode));
		} catch (Exception e) {
			Throwables.propagate(e);
		}
		return dataNode;
	}

	private void markUnused(String node) {
		try {
			zk.inTransaction().delete(paths.used().concat("/").concat(node))
			        .create(paths.unused().concat("/").concat(node)).commit();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	private boolean markUsed(String child) {
		boolean success = false;
		Timer.Context ctx = metrics.timer(name(ZKObjectPool.class, "mark-used")).time();
		try {
			zk.inTransaction().delete(paths.unused().concat("/").concat(child))
			        .createEphemeral(paths.used().concat("/").concat(child)).commit();
			success = true;
		} catch (KeeperException.NoNodeException e) {
			// normal, may have missed out on getting the object to borrow.
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			ctx.stop();
			recordHitOrMiss(success);
		}
		return success;
	}

	private void recordHitOrMiss(boolean success) {
		if (success) {
			metrics.counter(("hit")).inc();
		} else {
			metrics.counter(("miss")).inc();
		}
	}

	private String find() {
		String node = findNode();
		if (node == null && !isFull()) {
			node = addNew(paths.used());
		}
		return node;
	}

	private String findNode() {
		List<String> unusedObjectPaths = null;
		String found = null;
		while ((unusedObjectPaths = unusedObjectPaths()).size() > 0) {
			int nodeIndex = selectNodeIndex(unusedObjectPaths);
			if (nodeIndex != -1) {
				String chosenNode = unusedObjectPaths.get(nodeIndex);
				if (markUsed(chosenNode)) {
					found = chosenNode;
					break;
				}
			} else {
				break;
			}
		}
		return found;
	}

	private int selectNodeIndex(List<String> unusedObjectPaths) {
		int used = getUsed();
		int unused = unusedObjectPaths.size();
		int limit = (int) (used * 0.4);
		limit = limit != 0 && limit < unused ? limit : unused;
		return limit > 0 ? (int) ((System.nanoTime() ^ Thread.currentThread().hashCode()) % limit) : -1;
	}

	private List<String> unusedObjectPaths() {
		return nodeSorter.sortedCopy(zk.getChildren(paths.unused()));
	}

	private byte[] createData() {
		return factory.serialize(factory.create());
	}

	private void checkBorrowed() {
		if (!borrowed.isEmpty()) {
			LOGGER.warn("There are {} objects still in use , proceeding with shutdown", borrowed.size());
		}
	}

	private void destroyAllObjects(int partcipantsLeft) throws InterruptedException, KeeperException {
		checkBorrowed();
		if (partcipantsLeft == 0) {
			LOGGER.info("No participants left in the pool, cleaning up.");
			for (String node : zk.getChildren(paths.master())) {
				T obj = getData(node);
				destroyObj(node, obj);
			}
			zk.inTransaction().deleteRecursive(paths.base()).commit();
		} else {
			LOGGER.info("There are still {} participants in the pool, leaving pool intact.", partcipantsLeft);
		}

	}

	private void deregister() {
		if (isRegistered()) {
			try {
				zk.delete(paths.participants() + "/" + id);
			} catch (NoNodeException e) {
				// nevermind we already dropped.
			}
		}
	}

	private boolean destroyObj(String node, T object) {
		boolean destroyed = false;
		try {
			factory.destroy(object);
			destroyed = true;
		} catch (ZombieException e) {
			if (!shutdown) {
				handleZombie(node);
			}
		} catch (Exception e) {
			LOGGER.error("Error destroying the object {} due to {}", object, e);
		}
		return destroyed;
	}

}
