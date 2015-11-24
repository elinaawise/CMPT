package cmpt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Test3 {

  public static void runPerfTest(Connector conn, String tableName) throws Exception {

    try{
      conn.tableOperations().delete(tableName);
    } catch(TableNotFoundException e){}

    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName,Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1);

    boolean randomize = true;

    timeX(bw, bs, null, randomize);

    double rateSum = 0;

    for(int i = 1; i< 20; i++) {
      rateSum += timeX(bw, bs, (long)i, randomize);
    }

    System.out.printf("rate avg : %6.2f \n", rateSum/20);

    System.out.println("Flushing");
    conn.tableOperations().flush(tableName, null, null, true);

    rateSum = 0;

    for(int i = 20; i< 40; i++) {
      rateSum += timeX(bw, bs, (long)i, randomize);
    }

    System.out.printf("rate avg : %6.2f \n", rateSum/20);
  }

  private static double timeX(BatchWriter bw, BatchScanner bs, Long seq, boolean randomize) throws Exception {
    ArrayList<Range> ranges = new ArrayList<>();

    Mutation cm = new Mutation("r01");

    ArrayList<Integer> ints = new ArrayList<>(10000);

    for(int i = 0; i < 10000; i++) {
      ints.add(i);
    }

    if(randomize){
      Collections.shuffle(ints);
    }

    for(int i = 0; i < 10000; i++) {
      String qual = String.format("q%07d", ints.get(i));
      cm.put("seq", qual, seq == null ? "1" : (seq +1)+"");
      //look between existing values
      ranges.add(Range.exact("r01", "seq", qual+".a"));
    }


    long t1 = System.currentTimeMillis();
    bw.addMutation(cm);
    bw.flush();

    long t2 = System.currentTimeMillis();

    bs.setRanges(ranges);

    int count = 0;
    for (Entry<Key,Value> entry : bs) {
      count++;
    }
    if(0 != count) {throw new RuntimeException("count = "+count);}

    long t3 = System.currentTimeMillis();


    double rate1 = 10000 / ((t2 -t1)/1000.0);
    double rate2 = 10000 / ((t3 -t2)/1000.0);
    System.out.printf("time: %d ms  rate : %6.2f writes/sec %6.2f reads/sec \n", (t2 - t1), rate1, rate2);
    return rate2;
  }

  public static void main(String[] args) throws Exception  {
    PropertiesConfiguration config = new PropertiesConfiguration(args[0]);
    ZooKeeperInstance zki = new ZooKeeperInstance(config);
    Connector conn = zki.getConnector(config.getString("user.name"), new PasswordToken(config.getString("user.password")));
    runPerfTest(conn, "foo");
  }
}
