package maming;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

public class GoogleUtil {

  public void test1(){
    
    Set<String> setSon = new HashSet<String>();
    setSon.add("aaaa");
    setSon.add("ccc");
    
    Set<String> setFather = new HashSet<String>();
    setFather.add("ccc");
    setFather.add("aaaa");
    setFather.add("bbb");
    
    Set<String> diff = Sets.difference(setSon,setFather);
    
    System.out.println(diff);
  }
  
  public static void main(String[] args) {
    GoogleUtil test = new GoogleUtil();
    test.test1();
  }
}
