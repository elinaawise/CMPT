package cmpt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;

public class Test4 {
  public static void runPerfTest(Connector conn, String tableName) throws Exception {

    try {
      conn.tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
    conn.tableOperations().setProperty(tableName, Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    conn.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "8K");
    conn.tableOperations().setLocalityGroups(tableName, ImmutableMap.of("lg1", ImmutableSet.of(new Text("ntfy"))));

    int numRows = 100000;
    int numCols = 100;
    int numTest = 10;

    writeData(conn, tableName, numRows, numCols);
    writeLgData(conn, tableName, numRows);

    ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig());

    double rateSum = 0;

    for (int i = 0; i < numTest; i++) {
      rateSum += timeX(cw, numRows, numCols);
    }

    System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);

    System.out.println("Flushing");
    conn.tableOperations().flush(tableName, null, null, true);

    rateSum = 0;

    for (int i = 0; i < numTest; i++) {
      rateSum += timeX(cw, numRows, numCols);
    }

    System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);

    System.out.println("Compacting");
    conn.tableOperations().compact(tableName, null, null, true, true);

    rateSum = 0;

    for (int i = 0; i < numTest; i++) {
      rateSum += timeX(cw, numRows, numCols);
    }

    System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);
  }

  private static void writeData(Connector conn, String tableName, int numRows, int numCols) throws TableNotFoundException, MutationsRejectedException {

    long t1 = System.currentTimeMillis();

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    for (int row = 0; row < numRows; row++) {
      bw.addMutation(genRow(row, numCols));
    }

    bw.close();
    long t2 = System.currentTimeMillis();

    double rate = numRows * numCols / ((t2 - t1) / 1000.0);
    System.out.printf("time: %d ms  rate : %6.2f entries/sec written\n", (t2 - t1), rate);
  }

  private static Mutation genRow(int row, int numCols) {
    String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));

    Mutation m = new Mutation(r);

    for (int col = 0; col < numCols; col++) {
      String c = String.format("%04x", Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      m.put("data", c, "1");
    }

    return m;
  }

  private static void writeLgData(Connector conn, String tableName, int numRows) throws TableNotFoundException, MutationsRejectedException {

    long t1 = System.currentTimeMillis();

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    numRows = numRows * 10;

    for (int row = 0; row < numRows; row++) {
      String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));

      Mutation m = new Mutation(r);
      m.put("ntfy", "absfasf3", "");
      bw.addMutation(m);
    }

    bw.close();
    long t2 = System.currentTimeMillis();

    double rate = numRows / ((t2 - t1) / 1000.0);
    System.out.printf("time: %d ms  rate : %6.2f entries/sec written\n", (t2 - t1), rate);
  }

  private static String randRow(Random rand, int numRows) {
    int row = rand.nextInt(numRows);
    return String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));
  }

  private static Collection<String> randCols(Random rand, int num, int numCols) {
    HashSet<String> cols = new HashSet<String>();
    while (cols.size() < num) {
      int col = rand.nextInt(numCols);
      String c = String.format("%04x", Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      cols.add(c);
    }

    return cols;
  }

  private static double timeX(ConditionalWriter cw, int numRows, int numCols) throws Exception {

    Random rand = new Random();

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int row = 0; row < 3000; row++) {
      ConditionalMutation cm = new ConditionalMutation(randRow(rand, numRows));

      for (String col : randCols(rand, 10, numCols)) {
        cm.addCondition(new Condition("data", col).setValue("1"));
        cm.put("data", col, "1");
      }

      cmuts.add(cm);
    }

    long t1 = System.currentTimeMillis();

    int count = 0;
    Iterator<Result> results = cw.write(cmuts.iterator());
    while (results.hasNext()) {
      Result result = results.next();

      if (Status.ACCEPTED != result.getStatus()) {
        throw new RuntimeException();
      }
      count++;
    }

    if (cmuts.size() != count) {
      throw new RuntimeException();
    }
    long t2 = System.currentTimeMillis();

    double rate = 30000 / ((t2 - t1) / 1000.0);
    System.out.printf("time: %d ms  rate : %6.2f conditions/sec \n", (t2 - t1), rate);
    return rate;
  }

  public static void main(String[] args) throws Exception {
    PropertiesConfiguration config = new PropertiesConfiguration(args[0]);
    ZooKeeperInstance zki = new ZooKeeperInstance(config);
    Connector conn = zki.getConnector(config.getString("user.name"), new PasswordToken(config.getString("user.password")));
    runPerfTest(conn, "foo");
  }
}
