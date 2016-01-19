package maming;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;

public class TestBlockingQueue {

  public void test1(){
    BlockingQueue<String> pendingContainers = new LinkedBlockingQueue<String>();;
    pendingContainers.add("sss");
    
    Set<String> pendingContainerInThisCycle = new HashSet<String>();
    pendingContainers.drainTo(pendingContainerInThisCycle);
    System.out.println(pendingContainerInThisCycle);
    System.out.println("pendingContainers:"+pendingContainers);
  }
  
  public String test2(int directoryNo){
      String relativePath = "";
      if (directoryNo > 0) {
        String tPath = Integer.toString(directoryNo - 1, 36);
        System.out.println(tPath);
        StringBuffer sb = new StringBuffer();
        if (tPath.length() == 1) {
          sb.append(tPath.charAt(0));
        } else {
          // this is done to make sure we also reuse 0th sub directory
          sb.append(Integer.toString(
            Integer.parseInt(tPath.substring(0, 1), 36) - 1,
            36));
        }
        for (int i = 1; i < tPath.length(); i++) {
          sb.append(Path.SEPARATOR).append(tPath.charAt(i));
        }
        relativePath = sb.toString();
      }
      System.out.println(relativePath);
      System.out.println("---"+getCacheDirectoryRoot(new Path(relativePath)));
      getDirectoryNumber(relativePath);
      return relativePath;
  }
  
  static int getDirectoryNumber(String relativePath) {
    String numStr = relativePath.replace("/", "");
    if (relativePath.isEmpty()) {
      return 0;
    }
    if (numStr.length() > 1) {
      // undo step from getRelativePath() to reuse 0th sub directory
      String firstChar = Integer.toString(
          Integer.parseInt(numStr.substring(0, 1),
              36) + 1, 36);
      numStr = firstChar + numStr.substring(1);
    }
    return Integer.parseInt(numStr, 36) + 1;
  }
  
  static int DIRECTORIES_PER_LEVEL = 36;
      
  public static Path getCacheDirectoryRoot(Path path) {
    while (path != null) {
      String name = path.getName();
      if (name.length() != 1) {
        return path;
      }
      int dirnum = DIRECTORIES_PER_LEVEL;
      try {
        dirnum = Integer.parseInt(name, DIRECTORIES_PER_LEVEL);
      } catch (NumberFormatException e) {
      }
      if (dirnum >= DIRECTORIES_PER_LEVEL) {
        return path;
      }
      path = path.getParent();
    }
    System.out.println("path:"+path);
    return path;
  }
  
  public void test2(){

    Pattern RANDOM_DIR_PATTERN = Pattern.compile("-?\\d+");
    System.out.println(RANDOM_DIR_PATTERN.matcher("45").matches());
    System.out.println(RANDOM_DIR_PATTERN.matcher("-45").matches());
    System.out.println(RANDOM_DIR_PATTERN.matcher("-?45").matches());
  }
  
  public CacheLoader<String,Future<String>> test3(){
    return new CacheLoader<String,Future<String>>() {
      public Future<String> load(String path) {
        try {
          Thread.sleep(5000);
          return Futures.immediateFuture(path.length()+"");
        } catch (Throwable th) {
          // report failures so it can be memoized
          return Futures.immediateFailedFuture(th);
        }
      }
    };
  }
  
  public void aa(){
      PathMatchingResourcePatternResolver match = new PathMatchingResourcePatternResolver();
      try {
      Resource[] re = match.getResources("classpath:org/springframework/util/ClassUtils.class");
      System.out.println(re.length);
      for(Resource r:re){
      System.out.println(r.getURL());
      }
      } catch (IOException e) {
      e.printStackTrace();
      }
  }
  public static void main(String[] args) throws Exception {
    TestBlockingQueue test = new TestBlockingQueue();
    CacheLoader<String,Future<String>> aa = test.test3();
    System.out.println("aa");
    System.out.println(aa.load("6656").get());
    System.out.println("xxx");
  }
}
