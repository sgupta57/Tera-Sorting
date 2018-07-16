import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import java.io.FileWriter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.Writer;
import java.io.BufferedWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class TeraSorting {

  public static class TokenMap
       extends Mapper<Object, Text, Text, Text>{

    private Text key_w = new Text();
    private Text key_value = new Text();

    public void map(Object key, Text value, Context contextwriter
                    ) throws IOException, InterruptedException
    {
     try
      {
      key_w.set(value.toString().substring(0,10));
      key_value.set(value.toString().substring(10));
      contextwriter.write(key_w, key_value);
     }
     catch(Exception e)
     {
        System.out.println(e);
     }

    }
  }

  public static class Reducer_TeraSort
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context contextwriter
                       ) throws IOException, InterruptedException {

      for (Text a : values) {
        contextwriter.write(key, a);
      }
    }
  }
   public static void main(String[] args) throws Exception
   {
    double time_of_start = System.currentTimeMillis();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TeraSorting");
    job.setJarByClass(TeraSorting.class);
    job.setMapperClass(TokenMap.class);
    job.setCombinerClass(Reducer_TeraSort.class);
    job.setReducerClass(Reducer_TeraSort.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    double time_of_end = System.currentTimeMillis();
    double totaltimetakeninsec = (time_of_end - time_of_start)/1000;
    String s = String.valueOf(totaltimetakeninsec);

    try (Writer writer = new BufferedWriter(new FileWriter("/Saurabh/hadoop/share/hadoop/test.txt")))
    {
    writer.write("Total time elapse in sec:"+s);
    }
    catch (IOException e)
    {
    e.printStackTrace();
    }
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
   if (job.waitForCompletion(false))
    {
        System.exit(1);
    }
   else
    {
        System.exit(0);
   }
  }
}