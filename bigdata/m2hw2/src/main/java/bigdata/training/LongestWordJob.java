package bigdata.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static bigdata.training.LongestWordJob.LongestWordsCounters.LONGEST_WORD_LENGTH;

/**
 * Created by Maksym_Panchenko on 4/12/2017.
 */
public class LongestWordJob {

    public enum LongestWordsCounters {
        LONGEST_WORD_LENGTH
    }

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable LENGTH = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f.,;:\"\'#!?+()");
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.trim().length() > 0) {
                    word.set(token);
                    LENGTH.set(token.length() * -1); // to reverse shuffling
                    context.write(LENGTH, word);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Counter counter = context.getCounter(LONGEST_WORD_LENGTH);
            JSONObject jsn = new JSONObject();
            int wordLength = key.get() * -1;
            if (counter.getValue() <= wordLength) {
                counter.setValue(wordLength);

                try {
                    jsn.put("length", wordLength);
                    List<String> words = new ArrayList<>();
                    values.forEach(text -> words.add(text.toString()));
                    jsn.put("words", words);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                context.write(new Text(jsn.toString()), null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest word");
        job.setJarByClass(LongestWordJob.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
