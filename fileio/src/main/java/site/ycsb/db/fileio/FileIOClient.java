package site.ycsb.db.fileio;

import site.ycsb.*;
import site.ycsb.fileiointerface.*;
import net.jcip.annotations.GuardedBy;
// import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileIOClient extends DB {
    static {
        // System.setProperty("java.library.path", "/home/lml/YCSB/fileio/src/main/native");
        // System.loadLibrary("JAVASPTAGFileIO");
        // System.load("/home/lml/YCSB/fileio/src/main/native/libJAVASPTAG.so");
        // System.load("/home/lml/YCSB/fileio/src/main/native/libSPTAGLib.so");
        System.load("/home/lml/YCSB/fileio/src/main/native/libJAVASPTAGFileIO.so");
    }
    @Override
    public void init() throws DBException {
        synchronized (FileIOClient.class) {
            String mode = getProperties().getProperty(MODE, "load");
            System.err.println("FileIOClient: init mode = " + mode);
            if (fileIO == null) {
                if (mode.equals("load")) {
                    fileIO = new FileIOInterface("/mnt/nvme0n1/lml/fileio/fileio", 4096, 16000000, 5, 1024, 64, false, 1);
                }
                else {
                    fileIO = new FileIOInterface("/mnt/nvme0n1/lml/fileio/fileio", 4096, 16000000, 5, 1024, 64, true, 1);
                    try (FileInputStream fis = new FileInputStream("/mnt/nvme0n1/lml/fileio/fileioHashMap");
                        ObjectInputStream in = new ObjectInputStream(fis)) {
                        keyMap = (HashMap<String, Integer>) in.readObject();
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    try (FileInputStream fis = new FileInputStream("/mnt/nvme0n1/lml/fileio/fileioLinkedList");
                        ObjectInputStream in = new ObjectInputStream(fis)) {
                            key_queue = (LinkedList<Integer>) in.readObject();
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    try (FileInputStream fis = new FileInputStream("/mnt/nvme0n1/lml/fileio/fileioInteger");
                        ObjectInputStream in = new ObjectInputStream(fis)) {
                            keyCount = (Integer) in.readObject();
                    }
                    catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                fileIO.GetStat();
            }
            boolean result = fileIO.Initialize();
            if (!result) {
                throw new DBException("FileIO initialization failed");
            }
            references++;
        }
    }

    @Override
    public void cleanup() throws DBException {
        synchronized (FileIOClient.class) {
            fileIO.ExitBlockController();
            if(references == 1) {
                fileIO.GetStat();
                fileIO.Checkpoint("/mnt/nvme0n1/lml/fileio/fileio");
                try (FileOutputStream fos = new FileOutputStream("/mnt/nvme0n1/lml/fileio/fileioHashMap");
                    ObjectOutputStream out = new ObjectOutputStream(fos)) {
                    out.writeObject(keyMap);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try (FileOutputStream fos = new FileOutputStream("/mnt/nvme0n1/lml/fileio/fileioLinkedList");
                    ObjectOutputStream out = new ObjectOutputStream(fos)) {
                    out.writeObject(key_queue);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try (FileOutputStream fos = new FileOutputStream("/mnt/nvme0n1/lml/fileio/fileioInteger");
                    ObjectOutputStream out = new ObjectOutputStream(fos)) {
                    out.writeObject(keyCount);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            references--;
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        // String input = key.replaceFirst("^user0*", "");
        // int keyHash = 0;
        // if (!input.isEmpty()) {
        //     keyHash = Integer.parseInt(input);
        // }
        // byte[] value = fileIO.GetByteArray(keyHash);
        // if (value.length == 0) {
        //     System.err.println("FileIOClient: read failed");
        //     return Status.ERROR;
        // }
        Integer keyHash;
        lock.lock();
        keyHash = keyMap.get(key);
        if (keyHash == null) {
            return Status.NOT_FOUND;
        }
        lock.unlock();
        
        byte[] value = fileIO.GetByteArray(keyHash);
        if (value.length == 0) {
            System.err.println("FileIOClient: read failed");
            return Status.ERROR;
        }
        deserializeValues(value, fields, result);
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        Integer keyHash;
        lock.lock();
        keyHash = keyMap.get(startkey);
        if (keyHash == null) {
            return Status.NOT_FOUND;
        }
        lock.unlock();
        Vector<Byte []> values;
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        Integer keyHash;
        lock.lock();
        keyHash = keyMap.get(key);
        if (keyHash == null) {
            return Status.NOT_FOUND;
        }
        lock.unlock();

        byte[] value = fileIO.GetByteArray(keyHash);
        if (value.length == 0) {
            System.err.println("FileIOClient: update failed");
            return Status.ERROR;
        }
        final Map<String, ByteIterator> result = new HashMap<>();
        deserializeValues(value, null, result);
        result.putAll(values);
        try {
            byte[] serializedValues = serializeValues(result);
            boolean res = fileIO.Put(keyHash, serializedValues);
            if (!res) {
                return Status.ERROR;
            }
        } catch (IOException e) {
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        // String input = key.replaceFirst("^user0*", "");
        // int keyHash = 0;
        // if (!input.isEmpty()) {
        //     keyHash = Integer.parseInt(input);
        // }
        Integer keyHash;
        lock.lock();
        if (key_queue.isEmpty()) {
            keyHash = keyCount++;
        }
        else {
            keyHash = key_queue.poll();
        }
        keyMap.put(key, keyHash);
        lock.unlock();
        try {
            byte[] serializedValues = serializeValues(values);
            boolean result = fileIO.Put(keyHash, serializedValues);
            if (!result) {
                return Status.ERROR;
            }
        } catch (IOException e) {
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        // String input = key.replaceFirst("^user0*", "");
        // int keyHash = 0;
        // if (!input.isEmpty()) {
        //     keyHash = Integer.parseInt(input);
        // }
        Integer keyHash;
        lock.lock();
        keyHash = keyMap.get(key);
        if (keyHash == null) {
            return Status.ERROR;
        }
        keyMap.remove(key);
        key_queue.add(keyHash);
        lock.unlock();
        boolean result = fileIO.Delete(keyHash);
        if (!result) {
            return Status.ERROR;
        }
        return Status.OK;
    }

    private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
            final Map<String, ByteIterator> result) {
        final ByteBuffer buf = ByteBuffer.allocate(4);

        int offset = 0;
        while (offset < values.length) {
            buf.put(values, offset, 4);
            buf.flip();
            final int keyLen = buf.getInt();
            buf.clear();
            offset += 4;

            final String key = new String(values, offset, keyLen);
            offset += keyLen;

            buf.put(values, offset, 4);
            buf.flip();
            final int valueLen = buf.getInt();
            buf.clear();
            offset += 4;

            if (fields == null || fields.contains(key)) {
                result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
            }

            offset += valueLen;
        }

        return result;
    }

    private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final ByteBuffer buf = ByteBuffer.allocate(4);

            for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
                final byte[] keyBytes = value.getKey().getBytes(UTF_8);
                final byte[] valueBytes = value.getValue().toArray();

                buf.putInt(keyBytes.length);
                baos.write(buf.array());
                baos.write(keyBytes);

                buf.clear();

                buf.putInt(valueBytes.length);
                // if (valueBytes.length == 0) {
                //     System.err.println("FileIOClient: valueBytes.length is 0");
                // }
                // else {
                //     System.err.println("valueBytes.length is " + valueBytes.length);
                // }
                baos.write(buf.array());
                baos.write(valueBytes);

                buf.clear();
            }
            return baos.toByteArray();
        }
    }

    @GuardedBy("FileIOClient.class") private static FileIOInterface fileIO = null;
    @GuardedBy("FileIOClient.class") private static int references = 0;
    static final String MODE = "mode";
    private static HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
    private static int keyCount = 0;
    private static LinkedList<Integer> key_queue = new LinkedList<Integer>();
    private static ReentrantLock lock = new ReentrantLock();
}