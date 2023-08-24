import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixMultiplication {

  public static class Map extends Mapper<Object, Text, Text, Text> {

    private int m;
    private int p;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      m = Integer.parseInt(conf.get("m"));
      p = Integer.parseInt(conf.get("p"));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    

      String line = value.toString();
      
      
      String[] input = line.toString().replaceAll("\\s+", "").split(",");
 
      
      String matrixName = input[0];
      int i = Integer.parseInt(input[1]);
      int j = Integer.parseInt(input[2]);

      int matrixValue = Integer.parseInt(input[3]);
      

      if (matrixName.equals("A")) {
        for (int k = 0; k < p; k++) {
          context.write(new Text(i + "," + k), new Text(matrixName + "," + j + "," + matrixValue));
        }
      } else {
        for (int k = 0; k < m; k++) {
          context.write(new Text(k + "," + j), new Text(matrixName + "," + i + "," + matrixValue));
        }
      }
      
    }
  }
  

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] value;
		Configuration conf = context.getConfiguration();
		HashMap<Integer, Integer> hashA = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> hashB = new HashMap<Integer, Integer>();
		
		for (Text val : values) {
			 String line = val.toString();
			 value = line.toString().replaceAll("\\s+", "").split(",");
			if (value[0].equals("A")) {
				hashA.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
			} else {
				hashB.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
			}
		}
		int n = Integer.parseInt(context.getConfiguration().get("n"));
		int result = 0;
		for (int j = 0; j < n; j++) {
	
			result += hashA.get(j) * hashB.get(j);
		}
		
			context.write(key, new IntWritable(result));
			 
		
	}
}
	public static void main(String[] args) throws Exception { 
    	
    	Configuration conf = new Configuration();
        // M is an m-by-n matrix; N is an n-by-p matrix.
		conf.set("m", args[0]);
		conf.set("n", args[1]);
		conf.set("p", args[2]);
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

	//job.setInputFormatClass(TextInputFormat.class);    
    	//job.setOutputFormatClass(TextOutputFormat.class);         
 
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
 
        job.waitForCompletion(true);
    
  }
}
