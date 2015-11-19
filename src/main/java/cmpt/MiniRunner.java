package cmpt;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class MiniRunner {
  public static void main(String[] args) throws Exception {

    if(args.length != 2){
      System.out.println("Usage : "+MiniRunner.class.getName()+" <mac dir> <output props file>");
    }

    // its important to use dfs for group commit performance since hsync() calls are made when using dfs. The calls to hsync() are slow.
    MiniDFSCluster dfscluster = new MiniDFSCluster.Builder(new Configuration()).build();
    //System.out.println(dfscluster.getURI());

    MiniAccumuloConfig macConfig = new MiniAccumuloConfig(new File(args[0]), "secret");
    Map<String, String> site = new HashMap<String, String>();
    //setting instance.volumes was not working
    site.put("instance.dfs.uri", dfscluster.getURI().toString());
    site.put("tserver.memory.maps.max", "512M");
    //site.put("tserver.server.threads.minimum", "128");
    macConfig.setSiteConfig(site);
    macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
    //System.out.println(site);

    MiniAccumuloCluster mac = new MiniAccumuloCluster(macConfig);
    mac.start();

    FileWriter fw = new FileWriter(args[1]);
    fw.append("instance.zookeeper.host="+mac.getZooKeepers()+"\n");
    fw.append("instance.name="+mac.getInstanceName()+"\n");
    fw.append("user.name=root\n");
    fw.append("user.password=secret\n");
    fw.close();

    System.out.println("Wrote "+args[1]);
  }
}
