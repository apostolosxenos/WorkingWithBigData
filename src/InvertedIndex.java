import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndex {

    public static class WordMapper extends Mapper<Object, Text, Text, Text> {

        private int n;

        // Method that sets up to the program the minimum word's length
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = Integer.parseInt(conf.get("N"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String lineWithoutPunctuation = value.toString().replaceAll("[^a-zA-Z ]", " ");
            String inCaseSensitiveLine = lineWithoutPunctuation.toLowerCase();
            StringTokenizer iterator = new StringTokenizer(inCaseSensitiveLine);

            while (iterator.hasMoreTokens()) {
                String nextWord = iterator.nextToken().trim();
                // Word with length greater than user's input will be added to the context
                if (nextWord.length() >= n) {
                    context.write(new Text(nextWord), new Text(fileName));
                }
            }
        }
    }

    public static class WordFileNameSumReducer extends Reducer<Text, Text, Text, Text> {

        private Set<String> fileNames;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            fileNames = new HashSet<String>();

            // Deduplicates files' names
            for (Text val : values) {
                fileNames.add(val.toString());
            }

            context.write(key, new Text(fileNames.toString()));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 3) {
            throw new IllegalArgumentException("Usage <in> <out> <word's min length>");
        }

        Configuration conf = new Configuration();
        conf.setInt("N", Integer.parseInt(args[2]));
        Job job = Job.getInstance(conf, "inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordFileNameSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}