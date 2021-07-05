import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class CovidStatistics {

    private final static String TEMPPATH = "/tmp/temp";
    private static double globalMean;
    private static final IntWritable ONE = new IntWritable(1);

    public static class DailyCasesMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] data = record.split(",");

            // Skips the line of titles
            if (!data[0].equals("dateRep")) {
                String date = data[0];
                int cases = Integer.parseInt(data[4]);

                context.write(new Text(date), new IntWritable(cases));
            }
        }
    }

    public static class MeanCasesPerDayReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            int countries = 0;
            for (IntWritable val : values) {
                sum += val.get();
                countries++;
            }

            double dailyCasesMean = (double) sum / countries;

            context.write(key, new DoubleWritable(dailyCasesMean));
        }
    }

    private static double readAndCalcMean(Path path, Configuration conf) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = null;
        double totalCases = 0.0d;
        int days = 0;
        try {
            Path file = new Path(path, "part-r-00000");
            if (!fs.exists(file))
                throw new IOException("Output not found!");

            br = new BufferedReader(new InputStreamReader(fs.open(file)));
            String line;
            while ((line = br.readLine()) != null) {

                // Splits by white spaces
                double totalCasesPerDay = Double.parseDouble(line.split("\\s+")[1]);
                totalCases += totalCasesPerDay;
                days++;
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }

        double mean = ((totalCases) / ((double) days));
        System.out.println("The mean is: " + mean);

        return mean;
    }


    public static class AboveMeanCasesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private double mean;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            mean = Double.parseDouble(conf.get("GLOBAL_MEAN"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] data = record.split(",");

            // Skips the first line of titles
            if (!data[0].equals("dateRep")) {
                if (data[2] != null && data[3] != null && data[6] != null && data[4] != null) {

                    int cases = Integer.parseInt(data[4]);

                    if (cases > mean) {
                        String month = data[2];
                        String year = data[3];
                        String country = data[6];
                        String aboveMeanRecord = year + "-" + month + "-" + country;
                        context.write(new Text(aboveMeanRecord), ONE);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // First MapReduce Round -> Cases per day count
        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "cases per day count");
        firstJob.setJarByClass(Ypoergasia3.class);
        firstJob.setMapperClass(DailyCasesMapper.class);
        firstJob.setReducerClass(MeanCasesPerDayReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        Path tempDir = new Path(TEMPPATH);
        FileOutputFormat.setOutputPath(firstJob, tempDir);

        boolean result = firstJob.waitForCompletion(true);

        // Global mean calculation by parsing output file "part-r-00000"
        globalMean = readAndCalcMean(tempDir, conf);

        // Deletes temp file
        FileSystem.get(conf).delete(tempDir, true);

        // Set the global mean to configuration
        conf.setDouble("GLOBAL_MEAN", globalMean);

        // Second MapReduce Round -> Above mean
        Job secondJob = Job.getInstance(conf);
        secondJob.setJobName("above mean count");
        secondJob.setJarByClass(CovidStatistics.class);
        secondJob.setMapperClass(AboveMeanCasesMapper.class);
        secondJob.setReducerClass(IntSumReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(secondJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));
        result = secondJob.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}