
package hadoop.project;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVReader;

public class HBaseInserterForRatings {
    private static final String HTABLE_RATING = "Rating";
    private static final String HTABLE_FAMILY = "content";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(HBaseInserterForRatings.class);
        job.setMapperClass(MovieInsertToHBaseMapper.class);
        // No need for reducer.
        // job.setReducerClass(MovieSimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.addInputPath(job, new Path(
//                "/Users/Sam/Downloads/ml-latest-small/ratings.csv"));
//        FileOutputFormat.setOutputPath(job, new Path("outputInserterRating"));
        // Create tables if not exist.
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);
        if (!admin.tableExists(HTABLE_RATING)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(HTABLE_RATING);
            tableDescriptor.addFamily(new HColumnDescriptor(HTABLE_FAMILY));
            admin.createTable(tableDescriptor);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper class to insert into HBase.
     */
    public static class MovieInsertToHBaseMapper extends Mapper<Object, Text, Text, Text> {

        private String movieID = null;
        private String userID = null;
        private String rating = null;
        private Configuration config;
        private HTable ratingTable;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            config = HBaseConfiguration.create();
            ratingTable = new HTable(config, HTABLE_RATING);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                userID = nextLine[0];
                movieID = nextLine[1];
                rating = nextLine[2];
                // inserts into HBase
                Put p = new Put(Bytes.toBytes(movieID + "-" + userID));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Rating"),
                        Bytes.toBytes(rating));
                ratingTable.put(p);
            }
        }
    }
}
