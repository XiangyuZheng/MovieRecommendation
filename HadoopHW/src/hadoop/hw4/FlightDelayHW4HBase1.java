
package hadoop.hw4;

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

import com.opencsv.CSVReader;

public class FlightDelayHW4HBase1 {
    private static final String HTABLE_NAME = "Flight";
    private static final String HTABLE_FAMILY = "content";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(FlightDelayHW4HBase1.class);
        job.setMapperClass(FlightMapper.class);
        // no need for reducer for this job.
        // job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Create HTable "Flight" if not exists.
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);
        if (!admin.tableExists(HTABLE_NAME)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(HTABLE_NAME);
            tableDescriptor.addFamily(new HColumnDescriptor(HTABLE_FAMILY));
            admin.createTable(tableDescriptor);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper class to insert into HBase.
     */
    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputVal = new Text();
        private String year = null;
        private String month = null;
        private String carrier = null;
        private String date = null;
        private String flightNum = null;
        private String arrDelayMinutes = null;
        private String outputKeyStr = null;
        private String outputValStr = null;
        private Configuration config;
        private HTable table;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            config = HBaseConfiguration.create();
            table = new HTable(config, HTABLE_NAME);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                year = nextLine[0];
                // skips the first line
                if (year.equals("Year")) {
                    continue;
                }
                month = nextLine[2];
                date = nextLine[5];
                carrier = nextLine[6];
                flightNum = nextLine[10];
                arrDelayMinutes = nextLine[37];
                // checks if arrDelayMinutes is not missing
                if (arrDelayMinutes.equals("")) {
                    continue;
                }
                // inserts into HBase
                Put p = new Put(
                        Bytes.toBytes(year + (Integer.parseInt(month) < 10 ? "0" + month : month)
                                + carrier + date + flightNum));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Year"),
                        Bytes.toBytes(nextLine[0]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Quarter"),
                        Bytes.toBytes(nextLine[1]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Month"),
                        Bytes.toBytes(nextLine[2]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DayofMonth"),
                        Bytes.toBytes(nextLine[3]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DayOfWeek"),
                        Bytes.toBytes(nextLine[4]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("FlightDate"),
                        Bytes.toBytes(nextLine[5]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("UniqueCarrier"),
                        Bytes.toBytes(nextLine[6]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("AirlineID"),
                        Bytes.toBytes(nextLine[7]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Carrier"),
                        Bytes.toBytes(nextLine[8]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("TailNum"),
                        Bytes.toBytes(nextLine[9]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("FlightNum"),
                        Bytes.toBytes(nextLine[10]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Origin"),
                        Bytes.toBytes(nextLine[11]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("OriginCityName"),
                        Bytes.toBytes(nextLine[12]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("OriginState"),
                        Bytes.toBytes(nextLine[13]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("OriginStateFips"),
                        Bytes.toBytes(nextLine[14]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("OriginStateName"),
                        Bytes.toBytes(nextLine[15]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("OriginWac"),
                        Bytes.toBytes(nextLine[16]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Dest"),
                        Bytes.toBytes(nextLine[17]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DestCityName"),
                        Bytes.toBytes(nextLine[18]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DestState"),
                        Bytes.toBytes(nextLine[19]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DestStateFips"),
                        Bytes.toBytes(nextLine[20]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DestStateName"),
                        Bytes.toBytes(nextLine[21]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DestWac"),
                        Bytes.toBytes(nextLine[22]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("CRSDepTime"),
                        Bytes.toBytes(nextLine[23]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepTime"),
                        Bytes.toBytes(nextLine[24]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepDelay"),
                        Bytes.toBytes(nextLine[25]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepDelayMinutes"),
                        Bytes.toBytes(nextLine[26]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepDel15"),
                        Bytes.toBytes(nextLine[27]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepartureDelayGroups"),
                        Bytes.toBytes(nextLine[28]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DepTimeBlk"),
                        Bytes.toBytes(nextLine[29]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("TaxiOut"),
                        Bytes.toBytes(nextLine[30]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("WheelsOff"),
                        Bytes.toBytes(nextLine[31]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("WheelsOn"),
                        Bytes.toBytes(nextLine[32]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("TaxiIn"),
                        Bytes.toBytes(nextLine[33]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("CRSArrTime"),
                        Bytes.toBytes(nextLine[34]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrTime"),
                        Bytes.toBytes(nextLine[35]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrDelay"),
                        Bytes.toBytes(nextLine[36]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrDelayMinutes"),
                        Bytes.toBytes(nextLine[37]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrDel15"),
                        Bytes.toBytes(nextLine[38]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrivalDelayGroups"),
                        Bytes.toBytes(nextLine[39]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ArrTimeBlk"),
                        Bytes.toBytes(nextLine[40]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Cancelled"),
                        Bytes.toBytes(nextLine[41]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("CancellationCode"),
                        Bytes.toBytes(nextLine[42]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Diverted"),
                        Bytes.toBytes(nextLine[43]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("CRSElapsedTime"),
                        Bytes.toBytes(nextLine[44]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("ActualElapsedTime"),
                        Bytes.toBytes(nextLine[45]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("AirTime"),
                        Bytes.toBytes(nextLine[46]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Flights"),
                        Bytes.toBytes(nextLine[47]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Distance"),
                        Bytes.toBytes(nextLine[48]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("DistanceGroup"),
                        Bytes.toBytes(nextLine[49]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("CarrierDelay"),
                        Bytes.toBytes(nextLine[50]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("WeatherDelay"),
                        Bytes.toBytes(nextLine[51]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("NASDelay"),
                        Bytes.toBytes(nextLine[52]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("SecurityDelay"),
                        Bytes.toBytes(nextLine[53]));
                p.add(Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("LateAircraftDelay"),
                        Bytes.toBytes(nextLine[54]));
                table.put(p);
                outputKeyStr = "";
                outputValStr = "";
                outputKey.set(outputKeyStr);
                outputVal.set(outputValStr);
                context.write(outputKey, outputVal);
            }
        }
    }
}
