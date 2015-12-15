package hadoop.project.multiply;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.project.HBaseInserterForRatings.MovieInsertToHBaseMapper;

public class InsertJob {
    static final String HTABLE_SIMILARITY = "similarity";
    static final String HTABLE_RATING = "rating";
    static int MOVIE_COUNT = 30000 + 1;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(SimilarityInsert.class);
        job.setMapperClass(MovieInsertToHBaseMapper.class);
        // No need for reducer.
        // job.setReducerClass(MovieSimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // FileInputFormat.addInputPath(job, new Path(
        // "/Users/Sam/Downloads/ml-latest-small/ratings.csv"));
        // FileOutputFormat.setOutputPath(job, new
        // Path("outputInserterRating"));
        // Create tables if not exist.
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);
        if (!admin.tableExists(HTABLE_SIMILARITY)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(HTABLE_SIMILARITY);
            tableDescriptor.addFamily(new HColumnDescriptor(HTABLE_RATING));
            admin.createTable(tableDescriptor);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class SimilarityInsert {
        static class SimilarityInsertMapper extends Mapper<Object, Text, IntWritable, Text> {
            @Override
            protected void map(Object key, Text value,
                    Mapper<Object, Text, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {
                /* parse */
                String[] ss = value.toString().split("\t");
                System.err.println(ss);

                int row = Integer.parseInt(ss[0]);
                String col = ss[1];
                int simlarity = (int) (100 * Double.parseDouble(ss[2]));

                /* emit. */
                context.write(new IntWritable(row), new Text(col + '\t' + simlarity));
            }
        }

        static class SimilarityInsertPartitioner extends Partitioner<IntWritable, Text> {
            @Override
            public int getPartition(IntWritable key, Text val, int numPartition) {
                return key.get() % numPartition;
            }
        }

        static class SimilarityInsertReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            @Override
            protected void reduce(IntWritable key, Iterable<Text> vals,
                    Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {
                StringBuilder strRow = new StringBuilder();
                int[] intRow = new int[MOVIE_COUNT];

                /* write to file ; write to hbase */

            }
        }

    }

}
