package hadoop.project.multiply;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.project.multiply.InsertJob.SimilarityInsert.SimilarityInsertPartitioner;
import hadoop.project.multiply.InsertJob.SimilarityInsert.SimilarityInsertReducer;

public class InsertJob {
    static final String HTABLE_SIMILARITY = "similarity";
    static final String HTABLE_RATING = "rating";
    static int MOVIE_COUNT = 30000 + 1;
    /* give short name for performance */
    static final String CF_NAME = "d";
    static final String CQ_NAME = "d";
    static final byte[] CF_NAME_BYTE;
    static final byte[] CQ_NAME_BYTE;

    static {
        CF_NAME_BYTE = toBytes(CF_NAME);
        CQ_NAME_BYTE = toBytes(CQ_NAME);
    }

    // static {
    // /* Error checking with HBase connection! */
    // hbaseCon.addResource("/home/hadoop/hbase/conf/hbase-site.xml");
    // try {
    // System.out.println("zzzz connection test.");
    // HBaseAdmin.checkHBaseAvailable(hbaseCon);
    // } catch (Exception e) {
    // System.out.println("zzzz !! connection test fail!!");
    // e.printStackTrace();
    // }
    //
    // }

    // {
    // hbaseCon.set("hbase.zookeeper.quorum", "localhost");
    // }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("wrong size of args : " + args.length);
            System.exit(-1);
        }

        insertSimilarity(args[0], args[2] + "/Silimarity");
        transformRating(args[1], args[2] + "/Rating");
        multiply(args[2] + "/multiply");

    }

    static void multiply(String output) throws Exception {
        System.out.println("multiply +");
        Configuration hbaseCon = HBaseConfiguration.create();
        HBaseAdmin.checkHBaseAvailable(hbaseCon);
        Job job = new Job(hbaseCon, "MatrixMultiply");

        job.setJarByClass(Multiply.class); // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(toBytes(HTABLE_RATING), scan,
                Multiply.MultiplyMapper.class, IntWritable.class, Text.class, job);

        job.setReducerClass(Multiply.MultiplyReducer.class);

        // job.setOutputFormatClass(FileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output + System.currentTimeMillis() / 1000));

        if (!job.waitForCompletion(true))
            throw new RuntimeException("job failed!");

        System.out.println("multiply -");
    }

    static class Multiply {

        static class MultiplyMapper extends TableMapper<IntWritable, Text> {

            final int size = 200 * 1024;
            /* index is movie id, in below arrays. */
            int[] bufferRatings = new int[size];
            int[] bufferSimilarities = new int[size];
            int[] bufferResult = new int[size];

            HTable similarityTable;

            @Override
            protected void setup(
                    Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {

                Configuration hbaseCon = HBaseConfiguration.create();
                try {
                    HBaseAdmin.checkHBaseAvailable(hbaseCon);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException("still the same error", e);
                }

                similarityTable = new HTable(hbaseCon, HTABLE_SIMILARITY);
            }

            public void map(ImmutableBytesWritable row, Result result, Context context)
                    throws InterruptedException, IOException {
                /*
                 * get user's rating array
                 */

                int userID = Bytes.toInt(row.get());
                int[] ratings = parseKeyValue(result, bufferRatings);

                /*
                 * make a list of get.
                 */
                List<Get> gets = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    if (ratings[i] != 0) {
                        Get get = new Get(toBytes(i)).addColumn(CF_NAME_BYTE, CQ_NAME_BYTE);
                        gets.add(get);
                    }
                }

                /*
                 * calculate recommendation index for one user.
                 */
                Arrays.fill(bufferResult, 0);

                Result[] similaraties = similarityTable.get(gets);
                for (Result oneRow : similaraties) {
                    System.out.println(oneRow);
                }

                for (Result oneRow : similaraties) {
                    // now related similarity found for this user
                    if (oneRow.getRow() == null)
                        continue;
                    int[] similarity = parseKeyValue(oneRow, bufferSimilarities);
                    // get the rating for this similarity row.
                    int rating = ratings[Bytes.toInt(oneRow.getRow())];

                    // accumulate result
                    for (int i = 0; i < size; i++) {
                        bufferResult[i] += rating * similarity[i];
                    }
                }

                /*
                 * sort and output
                 */

                List<Pair> reco = getTop100(bufferResult);

                StringBuilder sb = new StringBuilder();
                for (Pair p : reco) {
                    sb.append('\t').append('(').append(p.idx).append('\t').append(p.value)
                            .append(')');
                }
                System.out.println(reco);

                context.write(new IntWritable(userID), new Text(sb.toString()));

            }

            static class Pair implements Comparable<Pair> {
                int idx;
                int value;

                public Pair(int i, int v) {
                    idx = i;
                    value = v;
                }

                @Override
                /**
                 * decreading order
                 */
                public int compareTo(Pair that) {
                    return that.value - this.value;
                }

                @Override
                public String toString() {

                    return "" + value;
                }
            }

            List<Pair> getTop100(int[] result) {
                Pair[] array = new Pair[result.length];
                for (int i = 0; i < result.length; i++)
                    array[i] = new Pair(i, result[i]);

                // decreasing order.
                Arrays.sort(array);

                List<Pair> ret = new ArrayList<>(100);
                for (int i = 0; i < 100; i++) {
                    ret.add(array[i]);
                }

                return ret;
            }

            private int[] parseKeyValue(Result result, int[] buffer) {
                Arrays.fill(buffer, 0);
                KeyValue cell = result.getColumnLatest(CF_NAME_BYTE, CQ_NAME_BYTE);
                if (cell == null)
                    return buffer;
                String s = new String(cell.getValue());

                // build map. movieid -> rating ; map avg size == 50
                StringTokenizer st = new StringTokenizer(s, "()\t");

                while (st.hasMoreTokens()) {
                    int movieID = Integer.parseInt(st.nextToken());
                    int rating = Integer.parseInt(st.nextToken());
                    buffer[movieID] = rating;
                }

                return buffer;
            }
        }

        static class MultiplyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            String msg = "MultiplyReducer reduce";

            @Override
            protected void reduce(IntWritable arg0, Iterable<Text> arg1,
                    Reducer<IntWritable, Text, IntWritable, Text>.Context arg2)
                            throws IOException, InterruptedException {
                System.out.println(this + "\t" + arg0 + msg + "+");
                super.reduce(arg0, arg1, arg2);
                System.out.println(this + "\t" + arg0 + msg + "-");
            }
        }
    }

    public static void insertSimilarity(String input, String output) throws Exception {
        /*
         * read similarity input and write to HBase.
         */
        System.out.println("insertSimilarity +");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "insertSimilarity");
        job.setJarByClass(SimilarityInsert.class);
        job.setMapperClass(SimilarityInsert.SimilarityInsertMapper.class);
        job.setReducerClass(SimilarityInsert.SimilarityInsertReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output + System.currentTimeMillis() / 1000));

        // create if table not exist
        createTable(HTABLE_SIMILARITY, CF_NAME);
        if (!job.waitForCompletion(true))
            throw new RuntimeException("job failed!");
        System.out.println("insertSimilarity -");
        // readTest(HTABLE_SIMILARITY);
    }

    public static void transformRating(String input, String output) throws Exception {
        System.out.println("transformRating +");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "transformRating");
        job.setJarByClass(RatingInsert.class);
        job.setMapperClass(RatingInsert.RatingInsertMapper.class);
        job.setReducerClass(RatingInsert.RatingInsertReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output + System.currentTimeMillis() / 1000));

        // create if table not exist
        createTable(HTABLE_RATING, CF_NAME);
        if (!job.waitForCompletion(true))
            throw new RuntimeException("job failed!");
        // readTest(HTABLE_RATING);
        System.out.println("transformRating -");
    }

    // try to dump content in htable, print out in string.
    static void readTest(String table) throws IOException {
        System.out.println("readTest");
        Configuration hbaseCon = HBaseConfiguration.create();

        try {
            HBaseAdmin.checkHBaseAvailable(hbaseCon);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("still the same error", e);
        }

        HTable hTable = new HTable(hbaseCon, table);

        Scan scan = new Scan();
        ResultScanner sc = hTable.getScanner(scan);

        for (Result row : sc) {
            // TODO use old fashioned api
            KeyValue cell = row.getColumnLatest(CF_NAME_BYTE, CQ_NAME_BYTE);
            String s = new String(cell.getValue());
            System.out.println(s);
        }

        hTable.close();

    }

    private static void createTable(String table, String cFamily)
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        // Instantiating configuration class

        Configuration hbaseCon = HBaseConfiguration.create();
        try {
            HBaseAdmin.checkHBaseAvailable(hbaseCon);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("still the same error", e);
        }

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(hbaseCon);

        // delete previously created table.
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        // Instantiating table descriptor class
        // below line not supported in AWS
        /*
         * HTableDescriptor tableDescriptor = new
         * HTableDescriptor(TableName.valueOf(table));
         */
        HTableDescriptor tableDescriptor = new HTableDescriptor(table);

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor(cFamily));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        admin.close();
        System.out.println(" Table created :" + table);

    }

    static class SimilarityInsert {
        static class SimilarityInsertMapper extends Mapper<Object, Text, IntWritable, Text> {
            @Override
            /**
             * emit (row, (col value))
             */
            protected void map(Object key, Text value,
                    Mapper<Object, Text, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {
                /* parse */
                String[] ss = value.toString().split("\\s+");
                // System.out.println(ss);

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

            List<Put> batchPut = new ArrayList<>();
            String msg = "SimilarityInsertReducer reduce";

            @Override
            protected void reduce(IntWritable key, Iterable<Text> vals,
                    Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {
                System.out.println(this + "\t" + key + msg + "+");

                StringBuilder strRow = new StringBuilder();

                /* parse value for integer and make string for file write */
                for (Text text : vals) {
                    String val = text.toString();
                    /* (col value) */
                    strRow.append('\t').append('(').append(val).append(')');
                    String[] ss = val.split("\\s+");
                    // System.out.println(Arrays.asList(ss));
                }

                /* write to file ; write to hbase */
                final String rowValue = strRow.toString();
                context.write(key, new Text(rowValue));

                byte[] rowkey = toBytes(key.get());
                Put put = new Put(rowkey).add(CF_NAME_BYTE, CQ_NAME_BYTE, rowValue.getBytes());

                batchPut.add(put);
                System.out.println(this + "\t" + key + msg + "-");
            }

            @Override
            protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                    throws IOException, InterruptedException {
                System.out.println(this + "\t" + "cleanup" + msg + "+");

                Timer t = new Timer().start();
                Configuration hbaseCon = HBaseConfiguration.create();
                try {
                    HBaseAdmin.checkHBaseAvailable(hbaseCon);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException("still the same error", e);
                }

                HTable hTable = new HTable(hbaseCon, HTABLE_SIMILARITY);
                hTable.put(batchPut);
                System.out.println("time spent on insert " + t.end());

                hTable.close();
                System.out.println(this + "\t" + "cleanup" + msg + "-");
            }
        }

    }

    static class RatingInsert {
        static class RatingInsertMapper extends Mapper<Object, Text, IntWritable, Text> {
            @Override
            /**
             * Line sample 9,2676,1.0,970889359
             * 
             * @param key
             * @param value
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */
            protected void map(Object key, Text value,
                    Mapper<Object, Text, IntWritable, Text>.Context context)
                            throws IOException, InterruptedException {
                try {
                    String[] ss = value.toString().split(",");
                    int userID = Integer.parseInt(ss[0]);
                    String movieID = ss[1];
                    int rating = (int) (Float.parseFloat(ss[2]) * 2);

                    /* emit. */
                    context.write(new IntWritable(userID), new Text(movieID + '\t' + rating));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        }

        static class RatingInsertPartitioner extends SimilarityInsertPartitioner {

        }

        /**
         * SimilarityInsertReducer is very similar to RatingInsertReducer. So,
         * just simply extend; only difference is setup.
         */
        static class RatingInsertReducer extends SimilarityInsertReducer {
            String msg = "RatingInsertReducer reduce";

            @Override
            protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                    throws IOException, InterruptedException {
                System.out.println(this + "\t" + "cleanup" + msg + "+");

                Timer t = new Timer().start();
                Configuration hbaseCon = HBaseConfiguration.create();
                try {
                    HBaseAdmin.checkHBaseAvailable(hbaseCon);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException("still the same error", e);
                }

                HTable hTable = new HTable(hbaseCon, HTABLE_RATING);
                hTable.put(batchPut);
                System.out.println("time spent on insert " + t.end());

                hTable.close();
                System.out.println(this + "\t" + "cleanup" + msg + "-");

            }
        }

    }
}

class Timer {
    Long start;

    Timer() {
    };

    Timer start() {
        start = System.currentTimeMillis();
        return this;
    }

    long end() {
        return System.currentTimeMillis() - start;
    }
}