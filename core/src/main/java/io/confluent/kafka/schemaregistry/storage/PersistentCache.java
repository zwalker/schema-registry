/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.utilint.Pair;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;

/**
 * Persistent cache
 */
public class PersistentCache<K extends Comparable<K>, V>
    implements LookupCache<K, V> {
  private static final Logger log = LoggerFactory.getLogger(PersistentCache.class);

  private final Serializer<K, V> serializer;
  private final String dataDir;
  private final int version;
  private final int cachePercentage;
  private Environment env;
  // visible for subclasses
  protected Database store;
  private SecondaryDatabase guidToSubjectVersions;
  private SecondaryDatabase hashToGuid;
  private SecondaryDatabase referencedBy;
  private boolean isOpen;

  public PersistentCache(
      Serializer<K, V> serializer, String dataDir, int version, int cachePercentage) {
    this.serializer = serializer;
    this.dataDir = dataDir;
    this.version = version;
    this.cachePercentage = cachePercentage;
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  public void init() throws StoreInitializationException {
    try {
      // Remove two versions ago
      File oldDir = new File(dataDir, "schemas-" + (version - 2));
      if (oldDir.exists()) {
        if (!deleteDirectory(oldDir)) {
          log.warn("Could not delete old dir: {}", oldDir);
        }
      }
      File dir = new File(dataDir, "schemas-" + version);
      if ((!dir.exists() && !dir.mkdirs()) || !dir.isDirectory() || !dir.canWrite()) {
        throw new StoreInitializationException(
            String.format(
                "Cannot access or write to directory [%s]", dir.getPath()));
      }
      open(dir, false);
    } catch (Exception e) {
      throw new StoreInitializationException("Initialization failed", e);
    }
  }

  private static boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  @Override
  public V get(K key) throws StoreException {
    try {
      byte[] keyBytes = serializer.serializeKey(key);
      DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
      DatabaseEntry dbValue = new DatabaseEntry();
      if (store.get(null, dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        byte[] valueBytes = dbValue.getData();
        return (V) serializer.deserializeValue(key, valueBytes);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new StoreException("Get failed, key: " + key, e);
    }
  }

  @Override
  public V put(K key, V value) throws StoreException {
    try {
      V oldValue = get(key);
      byte[] keyBytes = serializer.serializeKey(key);
      DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
      byte[] valueBytes = serializer.serializeValue(value);
      DatabaseEntry dbValue = new DatabaseEntry(valueBytes);
      store.put(null, dbKey, dbValue);
      return oldValue;
    } catch (Exception e) {
      throw new StoreException("Put failed, key: " + key + ", value: " + value, e);
    }
  }

  public CloseableIterator<V> getAll(K key1, K key2) throws StoreException {
    try {
      byte[] key1Bytes = serializer.serializeKey(key1);
      DatabaseEntry dbKey = new DatabaseEntry(key1Bytes);
      DatabaseEntry dbValue = new DatabaseEntry();
      Cursor cursor = store.openCursor(null, CursorConfig.READ_UNCOMMITTED); // avoid read locks

      return new CloseableIterator<V>() {
        private OperationStatus status;
        private V current;

        @Override
        public boolean hasNext() {
          if (current == null) {
            current = getNextEntry();
          }
          return current != null;
        }

        @Override
        public V next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          V next = current;
          current = null;
          return next;
        }

        private V getNextEntry() {
          try {
            if (status != null && status != OperationStatus.SUCCESS) {
              return null;
            }
            while (true) {
              if (status == null) {
                status = cursor.get(dbKey, dbValue, Get.SEARCH_GTE, null) == null
                    ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
              } else {
                status = cursor.get(dbKey, dbValue, Get.NEXT, null) == null
                    ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
              }
              if (status != OperationStatus.SUCCESS) {
                break;
              }
              K key = serializer.deserializeKey(Arrays.copyOfRange(
                  dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));

              if (key.compareTo(key2) >= 0) {
                status = OperationStatus.NOTFOUND;
                break;
              }

              V value = serializer.deserializeValue(key, Arrays.copyOfRange(
                  dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));

              return value;
            }
            return null;
          } catch (SerializationException e) {
            log.error("Failed to serialize", e);
            throw new RuntimeException(e);
          }
        }

        @Override
        public void close() {
          cursor.close();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public void putAll(Map<K, V> entries) throws StoreException {
    Transaction txn = env.beginTransaction(null, null);
    try {
      for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
        byte[] keyBytes = serializer.serializeKey(entry.getKey());
        DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
        byte[] valueBytes = serializer.serializeValue(entry.getValue());
        DatabaseEntry dbValue = new DatabaseEntry(valueBytes);
        store.put(txn, dbKey, dbValue);
      }
    } catch (Exception e) {
      txn.abort();
      throw new StoreException("Put all failed", e);
    }
    txn.commit();
  }

  @Override
  public V delete(K key) throws StoreException {
    try {
      V oldValue = get(key);
      byte[] keyBytes = serializer.serializeKey(key);
      DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
      store.delete(null, dbKey);
      return oldValue;
    } catch (Exception e) {
      throw new StoreException("Delete failed, key: " + key, e);
    }
  }

  @Override
  public CloseableIterator<K> getAllKeys() throws StoreException {
    try {
      DatabaseEntry dbKey = new DatabaseEntry();
      DatabaseEntry dbValue = new DatabaseEntry();
      Cursor cursor = store.openCursor(null, CursorConfig.READ_UNCOMMITTED); // avoid read locks

      return new CloseableIterator<K>() {
        private OperationStatus status;
        private K current;

        @Override
        public boolean hasNext() {
          if (current == null) {
            current = getNextEntry();
          }
          return current != null;
        }

        @Override
        public K next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          K next = current;
          current = null;
          return next;
        }

        private K getNextEntry() {
          try {
            if (status != null && status != OperationStatus.SUCCESS) {
              return null;
            }
            while (true) {
              status = cursor.getNext(dbKey, dbValue, LockMode.DEFAULT);
              if (status != OperationStatus.SUCCESS) {
                break;
              }
              K key = serializer.deserializeKey(Arrays.copyOfRange(
                  dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));

              return key;
            }
            return null;
          } catch (SerializationException e) {
            log.error("Failed to serialize", e);
            throw new RuntimeException(e);
          }
        }

        @Override
        public void close() {
          cursor.close();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public void flush() throws StoreException {
    try {
      env.flushLog(true);
    } catch (Exception e) {
      throw new StoreException("Failed to flush", e);
    }
  }

  @Override
  public void close() throws StoreException {
    try {
      if (isOpen) {
        referencedBy.close();
        hashToGuid.close();
        guidToSubjectVersions.close();
        store.close();
        isOpen = false;
      }
    } catch (Exception e) {
      throw new StoreException("Failed to close", e);
    }
  }

  @Override
  public SchemaIdAndSubjects schemaIdAndSubjects(Schema schema) throws StoreException {
    try (SecondaryCursor cursor = hashToGuid.openCursor(null, CursorConfig.READ_COMMITTED)) {
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
          = schema.getReferences();
      MD5 md5 = MD5.ofString(schema.getSchema(), refs == null ? null : refs.stream()
          .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
          .collect(Collectors.toList()));
      byte[] bytes = serializeHash(tenant(), md5);
      DatabaseEntry idxKey = new DatabaseEntry(bytes);
      DatabaseEntry dbKey = new DatabaseEntry();
      DatabaseEntry dbValue = new DatabaseEntry();
      int id = -1;
      Map<String, Integer> subjectVersions = new HashMap<>();
      OperationStatus status = cursor.getSearchKey(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      while (status == OperationStatus.SUCCESS) {
        SchemaRegistryKey key = (SchemaRegistryKey) serializer.deserializeKey(Arrays.copyOfRange(
            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));
        SchemaRegistryValue value = (SchemaRegistryValue) serializer.deserializeValue((K) key,
            Arrays.copyOfRange(
                dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          id = schemaValue.getId();
          subjectVersions.put(schemaValue.getSubject(), schemaValue.getVersion());
        }
        status = cursor.getNextDup(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      }
      if (subjectVersions.isEmpty()) {
        return null;
      }
      return new SchemaIdAndSubjects(id, subjectVersions);
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public boolean containsSchema(Schema schema) throws StoreException {
    return schemaIdAndSubjects(schema) != null;
  }

  @Override
  public Set<Integer> referencesSchema(SchemaKey schema) throws StoreException {
    try (SecondaryCursor cursor = referencedBy.openCursor(null, CursorConfig.READ_COMMITTED)) {
      byte[] keyBytes = serializer.serializeKey((K) schema);
      DatabaseEntry idxKey = new DatabaseEntry(keyBytes);
      DatabaseEntry dbKey = new DatabaseEntry();
      DatabaseEntry dbValue = new DatabaseEntry();
      Set<Integer> references = new HashSet<>();
      OperationStatus status = cursor.getSearchKey(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      while (status == OperationStatus.SUCCESS) {
        SchemaRegistryKey key = (SchemaRegistryKey) serializer.deserializeKey(Arrays.copyOfRange(
            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));
        SchemaRegistryValue value = (SchemaRegistryValue) serializer.deserializeValue((K) key,
            Arrays.copyOfRange(
                dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          if (!schemaValue.isDeleted()) {
            references.add(schemaValue.getId());
          }
        }
        status = cursor.getNextDup(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      }
      return references;
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public SchemaKey schemaKeyById(Integer id) throws StoreException {
    try {
      byte[] bytes = serializeGuid(tenant(), id);
      DatabaseEntry idxKey = new DatabaseEntry(bytes);
      DatabaseEntry dbKey = new DatabaseEntry();
      DatabaseEntry dbValue = new DatabaseEntry();
      if (guidToSubjectVersions.get(null, idxKey, dbKey, dbValue, LockMode.DEFAULT)
          == OperationStatus.SUCCESS) {
        byte[] keyBytes = dbKey.getData();
        return (SchemaKey) serializer.deserializeKey(keyBytes);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  public int getMaxId(String tenant) {
    try (SecondaryCursor cursor = guidToSubjectVersions.openCursor(
        null, CursorConfig.READ_COMMITTED)) {
      byte[] keyBytes = serializeGuid(tenant, Integer.MAX_VALUE);
      DatabaseEntry idxKey = new DatabaseEntry(keyBytes);
      DatabaseEntry dbKey = new DatabaseEntry();
      DatabaseEntry dbValue = new DatabaseEntry();
      OperationStatus status = cursor.getSearchKeyRange(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      if (status == OperationStatus.SUCCESS) {
        status = cursor.getPrev(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      } else {
        status = cursor.getLast(idxKey, dbKey, dbValue, LockMode.DEFAULT);
      }
      if (status == OperationStatus.SUCCESS) {
        Pair<String, Integer> pair = deserializeGuid(idxKey.getData());
        if (pair.first().equals(tenant)) {
          return pair.second();
        }
      }
    }
    return -1;
  }

  @Override
  public void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue) {
  }

  @Override
  public void schemaTombstoned(SchemaKey schemaKey, SchemaValue schemaValue) {
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompatibilityLevel compatibilityLevel(String subject,
                                               boolean returnTopLevelIfNotFound,
                                               CompatibilityLevel defaultForTopLevel
  ) throws StoreException {
    ConfigKey subjectConfigKey = new ConfigKey(subject);
    ConfigValue config = (ConfigValue) get((K) subjectConfigKey);
    if (config == null && subject == null) {
      return defaultForTopLevel;
    }
    if (config != null) {
      return config.getCompatibilityLevel();
    } else if (returnTopLevelIfNotFound) {
      config = (ConfigValue) get((K) new ConfigKey(null));
      return config != null ? config.getCompatibilityLevel() : defaultForTopLevel;
    } else {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mode mode(String subject,
                   boolean returnTopLevelIfNotFound,
                   Mode defaultForTopLevel
  ) throws StoreException {
    ModeKey modeKey = new ModeKey(subject);
    ModeValue modeValue = (ModeValue) get((K) modeKey);
    if (modeValue == null && subject == null) {
      return defaultForTopLevel;
    }
    if (modeValue != null) {
      return modeValue.getMode();
    } else if (returnTopLevelIfNotFound) {
      modeValue = (ModeValue) get((K) new ModeKey(null));
      return modeValue != null ? modeValue.getMode() : defaultForTopLevel;
    } else {
      return null;
    }
  }

  @Override
  public Set<String> subjects(String subject, boolean lookupDeletedSubjects) throws StoreException {
    return subjects(subject, matchingSubjectPredicate(subject), lookupDeletedSubjects);
  }

  private Set<String> subjects(
          String subject, Predicate<String> match, boolean lookupDeletedSubjects)
      throws StoreException {
    try (Cursor cursor = store.openCursor(null, CursorConfig.READ_COMMITTED)) {
      byte[] keyBytes = serializer.serializeKey((K) new SchemaKey(subject, 1));
      DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
      DatabaseEntry dbValue = new DatabaseEntry();
      Set<String> subjects = new HashSet<>();
      OperationStatus status = cursor.get(dbKey, dbValue, Get.SEARCH_GTE, null) == null
              ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
      while (status == OperationStatus.SUCCESS) {
        SchemaRegistryKey key = (SchemaRegistryKey) serializer.deserializeKey(Arrays.copyOfRange(
            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));
        if (!(key instanceof SubjectKey) || !match.test(((SubjectKey)key).getSubject())) {
          break;
        }

        SchemaRegistryValue value = (SchemaRegistryValue) serializer.deserializeValue((K) key,
            Arrays.copyOfRange(
                dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          if (!schemaValue.isDeleted() || lookupDeletedSubjects) {
            subjects.add(schemaValue.getSubject());
          }
        }
        status = cursor.getNext(dbKey, dbValue, LockMode.DEFAULT);
      }
      return subjects;
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public boolean hasSubjects(String subject, boolean lookupDeletedSubjects) throws StoreException {
    return hasSubjects(subject, matchingSubjectPredicate(subject), lookupDeletedSubjects);
  }

  private boolean hasSubjects(
          String subject, Predicate<String> match, boolean lookupDeletedSubjects)
      throws StoreException {
    return !subjects(subject, match, lookupDeletedSubjects).isEmpty();
  }

  @Override
  public void clearSubjects(String subject) throws StoreException {
    clearSubjects(subject, matchingSubjectPredicate(subject));
  }

  private void clearSubjects(String subject, Predicate<String> match) throws StoreException {
    Transaction txn = env.beginTransaction(null, null);
    try (Cursor cursor = store.openCursor(txn, CursorConfig.READ_COMMITTED)) {
      byte[] keyBytes = serializer.serializeKey((K) new SchemaKey(subject, 1));
      DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
      DatabaseEntry dbValue = new DatabaseEntry();
      OperationStatus status = cursor.get(dbKey, dbValue, Get.SEARCH_GTE, null) == null
              ? OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
      while (status == OperationStatus.SUCCESS) {
        SchemaRegistryKey key = (SchemaRegistryKey) serializer.deserializeKey(Arrays.copyOfRange(
            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));
        if (!(key instanceof SubjectKey) || !match.test(((SubjectKey)key).getSubject())) {
          break;
        }

        SchemaRegistryValue value = (SchemaRegistryValue) serializer.deserializeValue((K) key,
            Arrays.copyOfRange(
                dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          if (schemaValue.isDeleted()) {
            cursor.delete();
          }
        }
        status = cursor.getNext(dbKey, dbValue, LockMode.DEFAULT);
      }
    } catch (Exception e) {
      txn.abort();
      throw new StoreException(e);
    }
    txn.commit();
  }

  protected Predicate<String> matchingSubjectPredicate(String subject) {
    return s -> subject == null || subject.equals(s);
  }

  private void open(File envHome, boolean readOnly) throws DatabaseException {
    // Environment and database opens
    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setReadOnly(readOnly);
    envConfig.setAllowCreate(!readOnly);
    envConfig.setTransactional(true);
    // Data is synced in flush()
    envConfig.setDurability(Durability.COMMIT_NO_SYNC);
    envConfig.setCachePercent(cachePercentage);
    env = new Environment(envHome, envConfig);

    DatabaseConfig storeConfig = new DatabaseConfig();
    storeConfig.setReadOnly(readOnly);
    storeConfig.setAllowCreate(!readOnly);
    storeConfig.setTransactional(true);
    storeConfig.setBtreeComparator(new KeyComparator<>(serializer));
    storeConfig.setKeyPrefixing(true);
    store = env.openDatabase(null, "sr", storeConfig);

    GuidKeyCreator guidKeyCreator = new GuidKeyCreator();
    SecondaryConfig guidToSubjectVersionsConfig = new SecondaryConfig();
    guidToSubjectVersionsConfig.setReadOnly(readOnly);
    guidToSubjectVersionsConfig.setAllowCreate(!readOnly);
    guidToSubjectVersionsConfig.setTransactional(true);
    // Set up the secondary properties
    guidToSubjectVersionsConfig.setAllowPopulate(true); // Allow autopopulate
    guidToSubjectVersionsConfig.setKeyCreator(guidKeyCreator);
    // Need to allow duplicates for our secondary database
    guidToSubjectVersionsConfig.setSortedDuplicates(true);
    // Now open it
    guidToSubjectVersions = env.openSecondaryDatabase(
        null, "guidToSubjectVersions", store, guidToSubjectVersionsConfig);

    HashKeyCreator hashKeyCreator = new HashKeyCreator();
    SecondaryConfig hashToGuidConfig = new SecondaryConfig();
    hashToGuidConfig.setReadOnly(readOnly);
    hashToGuidConfig.setAllowCreate(!readOnly);
    hashToGuidConfig.setTransactional(true);
    // Set up the secondary properties
    hashToGuidConfig.setAllowPopulate(true); // Allow autopopulate
    hashToGuidConfig.setKeyCreator(hashKeyCreator);
    // Need to allow duplicates for our secondary database
    hashToGuidConfig.setSortedDuplicates(true);
    // Now open it
    hashToGuid = env.openSecondaryDatabase(
        null, "hashToGuid", store, hashToGuidConfig);

    ReferencedByKeyCreator referencedByKeyCreator = new ReferencedByKeyCreator();
    SecondaryConfig referencedByConfig = new SecondaryConfig();
    referencedByConfig.setReadOnly(readOnly);
    referencedByConfig.setAllowCreate(!readOnly);
    referencedByConfig.setTransactional(true);
    // Set up the secondary properties
    referencedByConfig.setAllowPopulate(true); // Allow autopopulate
    referencedByConfig.setMultiKeyCreator(referencedByKeyCreator);
    // Need to allow duplicates for our secondary database
    referencedByConfig.setSortedDuplicates(true);
    // Now open it
    referencedBy = env.openSecondaryDatabase(
        null, "referencedBy", store, referencedByConfig);
  }

  class GuidKeyCreator implements SecondaryKeyCreator {
    private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer =
        new SchemaRegistrySerializer();

    public boolean createSecondaryKey(SecondaryDatabase secDb,
        DatabaseEntry keyEntry,
        DatabaseEntry dataEntry,
        DatabaseEntry resultEntry) {
      try {
        SchemaRegistryKey key = serializer.deserializeKey(Arrays.copyOfRange(
            keyEntry.getData(), keyEntry.getOffset(), keyEntry.getOffset() + keyEntry.getSize()));
        SchemaRegistryValue value = serializer.deserializeValue(key, Arrays.copyOfRange(
            dataEntry.getData(), dataEntry.getOffset(),
            dataEntry.getOffset() + dataEntry.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          byte[] bytes = serializeGuid(tenant(), schemaValue.getId());
          resultEntry.setData(bytes);
          return true;
        } else {
          return false;
        }
      } catch (SerializationException e) {
        log.error("Failed to serialize", e);
        return false;
      }
    }
  }

  class HashKeyCreator implements SecondaryKeyCreator {
    private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer =
        new SchemaRegistrySerializer();

    public boolean createSecondaryKey(SecondaryDatabase secDb,
        DatabaseEntry keyEntry,
        DatabaseEntry dataEntry,
        DatabaseEntry resultEntry) {
      try {
        SchemaRegistryKey key = serializer.deserializeKey(Arrays.copyOfRange(
            keyEntry.getData(), keyEntry.getOffset(), keyEntry.getOffset() + keyEntry.getSize()));
        SchemaRegistryValue value = serializer.deserializeValue(key, Arrays.copyOfRange(
            dataEntry.getData(), dataEntry.getOffset(),
            dataEntry.getOffset() + dataEntry.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          String schema = schemaValue.getSchema();
          List<SchemaReference> refs = schemaValue.getReferences();
          MD5 md5 = MD5.ofString(schema, refs == null ? null : refs.stream()
              .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
              .collect(Collectors.toList()));
          byte[] bytes = serializeHash(tenant(), md5);
          resultEntry.setData(bytes);
          return true;
        } else {
          return false;
        }
      } catch (SerializationException e) {
        log.error("Failed to serialize", e);
        return false;
      }
    }
  }

  static class ReferencedByKeyCreator implements SecondaryMultiKeyCreator {
    private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer =
        new SchemaRegistrySerializer();

    public void createSecondaryKeys(SecondaryDatabase secDb,
        DatabaseEntry keyEntry,
        DatabaseEntry dataEntry,
        Set<DatabaseEntry> results) {
      try {
        SchemaRegistryKey key = serializer.deserializeKey(Arrays.copyOfRange(
            keyEntry.getData(), keyEntry.getOffset(), keyEntry.getOffset() + keyEntry.getSize()));
        SchemaRegistryValue value = serializer.deserializeValue(key, Arrays.copyOfRange(
            dataEntry.getData(), dataEntry.getOffset(),
            dataEntry.getOffset() + dataEntry.getSize()));
        if (value instanceof SchemaValue) {
          SchemaValue schemaValue = (SchemaValue) value;
          List<SchemaReference> refs = schemaValue.getReferences();
          for (SchemaReference ref : refs) {
            // Note that the version could be -1, so this cannot be a foreign key
            byte[] bytes =
                serializer.serializeKey(new SchemaKey(ref.getSubject(), ref.getVersion()));
            results.add(new DatabaseEntry(bytes));
          }
        }
      } catch (SerializationException e) {
        log.error("Failed to serialize", e);
      }
    }
  }

  public static class KeyComparator<K extends Comparable<K>, V>
      implements Comparator<byte[]>, Serializable {

    private static final long serialVersionUID = 8847023383654237176L;

    private final Serializer<K, V> serializer;

    public KeyComparator(Serializer<K, V> serializer) {
      this.serializer = serializer;
    }

    public int compare(byte[] b1, byte[] b2) {
      try {
        K key1 = serializer.deserializeKey(b1);
        K key2 = serializer.deserializeKey(b2);
        return key1.compareTo(key2);
      } catch (SerializationException e) {
        log.error("Failed to serialize", e);
        throw new RuntimeException(e);
      }
    }
  }

  static byte[] serializeGuid(String tenant, int guid) {
    byte[] tenantBytes = tenant.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length + 4);
    buf.putInt(tenantBytes.length);
    buf.put(tenantBytes);
    buf.putInt(guid);
    return buf.array();
  }

  static Pair<String, Integer> deserializeGuid(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data);
    int len = buf.getInt();
    byte[] tenantBytes = new byte[len];
    buf.get(tenantBytes);
    String tenant = new String(tenantBytes, StandardCharsets.UTF_8);
    int id = buf.getInt();
    return new Pair<>(tenant, id);
  }

  static byte[] serializeHash(String tenant, MD5 md5) {
    byte[] tenantBytes = tenant.getBytes(StandardCharsets.UTF_8);
    byte[] md5Bytes = md5.bytes();
    ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length + 4 + md5Bytes.length);
    buf.putInt(tenantBytes.length);
    buf.put(tenantBytes);
    buf.putInt(md5Bytes.length);
    buf.put(md5Bytes);
    return buf.array();
  }
}
