package maming;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationTest {

  private Configuration conf = new Configuration(); 
  
  
  public void test1(){
    conf.get("dfs.nfs.exports.allowed.hosts");
    conf.get("aadfs.nfs.exports.allowed.hosts");
  }
  
  public static void main(String[] args) {
    
    ConfigurationTest test = new ConfigurationTest();
    test.test1();
  }
  
}
