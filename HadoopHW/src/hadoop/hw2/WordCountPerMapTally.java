package hadoop.hw2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPerMapTally {

    // uses a hash map to store the "real" words and the partition indexes.
    public static Map<Character, Integer> map = new HashMap<Character, Integer>();
    static {
        map.put('m', 0);
        map.put('M', 0);
        map.put('n', 1);
        map.put('N', 1);
        map.put('o', 2);
        map.put('O', 2);
        map.put('p', 3);
        map.put('P', 3);
        map.put('q', 4);
        map.put('Q', 4);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountPerMapTally.class);
        job.setMapperClass(TokenizerMapper.class);
        // disables combiner
        // job.setCombinerClass(IntSumReducer.class);
        // sets partitioner
        job.setPartitionerClass(WordPartitioner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.addInputPath(job, new Path(
//                "/Users/Sam/Downloads/hw.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable num = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> tokenMap = new HashMap<String, Integer>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            String token = null;
            while (itr.hasMoreTokens()) {
                token = itr.nextToken();
                // skips "non-real" words
                if (!map.containsKey(token.charAt(0))) {
                    continue;
                }
                if (tokenMap.containsKey(token)) {
                    tokenMap.put(token, tokenMap.get(token) + 1);
                } else {
                    tokenMap.put(token, 1);
                }
            }
            for (Map.Entry<String, Integer> entry : tokenMap.entrySet()) {
                word.set(entry.getKey());
                num.set(entry.getValue());
                context.write(word, num);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    /**
     * Customized partitioner
     */
    public static class WordPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return map.get(key.toString().charAt(0)) % numPartitions;
        }
    }
}
