import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class MoviesKindCounter {

    public static class PairMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();

            // Splits record by '::'
            String[] recordParts = record.split("::");

            // Checks if record has the appropriate format consisting of 3 parts e.g. xxx::xxx::xxx
            if (recordParts.length == 3) {

                // Saves the last part [2]
                // For example it takes 'Documentary|Short' from the record
                // 0000008::Edison Kinetoscopic Record of a Sneeze (1894)::Documentary|Short
                String movieKindsPart = recordParts[2];

                // Splits again by special character '|'
                String[] movieKinds = movieKindsPart.split("\\|");
                
                int length = movieKinds.length;

                // If length of array is more than 1 we have at least one pair of kinds
                if (length > 1) {

                    // Sorting elements to avoid duplicate pairs (conceptually)
                    // For example a_b must be considered the same with b_a
                    Arrays.sort(movieKinds);

                    // Writes available pairs to context
                    for (int i = 0; i < length; i++) {
                        for (int j = i + 1; j < length; j++) {
                            String pair = movieKinds[i] + "_" + movieKinds[j];
                            context.write(new Text(pair), ONE);
                        }
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pair of movie kinds count");
        job.setJarByClass(MoviesKindCounter.class);
        job.setMapperClass(PairMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}