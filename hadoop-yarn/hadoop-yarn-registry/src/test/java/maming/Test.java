package maming;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Test {

  public void test1(String[] args) {
    Option recursive =
        OptionBuilder.withArgName("recursive").withDescription("delete recursively").create("r");

    Options rmOption = new Options();
    rmOption.addOption(recursive);

    boolean recursiveOpt = false;

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(rmOption, args);

      List<String> argsList = line.getArgList();
System.out.println(argsList.size());
      

      try {
        if (line.hasOption("r")) {
          System.out.println("--");
          recursiveOpt = true;
        }
      } catch (Exception e) {
      }
    } catch (ParseException exp) {
    }
  }

  public static void main(String[] args) {
    Test test = new Test();
    args = new String[] {"resolve","/aaa/bb","-r"};
    test.test1(args);
  }
}
