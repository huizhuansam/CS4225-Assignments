// Matric Number: A0219793N
// Name: Siew Hui Zhuan
// TopkCommonWords.java

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final String delimiter = " \t\n\r\f";
        private final Text word = new Text();
        private final IntWritable frequency = new IntWritable();
        private Set<String> stopWordSet;
        private Map<String, Integer> wordCounter;

        @Override
        public void setup(Context context) throws IOException {
            this.stopWordSet = new HashSet<>();
            this.wordCounter = new HashMap<>();
            URI[] stopWordFiles = context.getCacheFiles();

            if (stopWordFiles != null && stopWordFiles.length > 0) {
                for (URI stopWordFile : stopWordFiles) {
                    setupStopWords(stopWordFile);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), this.delimiter);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (!this.stopWordSet.contains(token)) {
                    if (!this.wordCounter.containsKey(token)) {
                        this.wordCounter.put(token, 0);
                    }
                    this.wordCounter.put(token, this.wordCounter.get(token) + 1);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : this.wordCounter.entrySet()) {
                this.word.set(entry.getKey());
                this.frequency.set(entry.getValue());
                context.write(this.word, this.frequency);
            }
        }

        private void setupStopWords(URI stopWordFile) {
            try (BufferedReader br = new BufferedReader(new FileReader(stopWordFile.toString()))) {
                String stopWord;
                while ((stopWord = br.readLine()) != null) {
                    this.stopWordSet.add(stopWord.strip());
                }
            } catch (IOException ioException) {
                System.err.println("Error reading " + stopWordFile.toString() + " " + ioException);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }

    public static class CommonWordsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final String separator = "\\t";
        private final String delimiter = System.lineSeparator();
        private final Text word = new Text();
        private final IntWritable frequency = new IntWritable();
        private Map<String, Integer> templateWordFreqMap;
        private Map<String, Integer> resultMap;

        @Override
        public void setup(Context context) throws IOException {
            this.templateWordFreqMap = new HashMap<>();
            this.resultMap = new HashMap<>();
            URI[] wordFreqFiles = context.getCacheFiles();
            if (wordFreqFiles != null && wordFreqFiles.length > 0) {
                for (URI wordFreqFile : wordFreqFiles) {
                    setupWordFreq(wordFreqFile);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), this.delimiter);
            while (itr.hasMoreTokens()) {
                String[] token = itr.nextToken().split(this.separator);
                String w = token[0];
                if (this.templateWordFreqMap.containsKey(w)) {
                    int freq1 = Integer.parseInt(token[1]);
                    int freq2 = this.templateWordFreqMap.get(w);
                    this.resultMap.put(w, Math.min(freq1, freq2));
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            this.resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(20)
                .forEach((entry) -> {
                    String k = entry.getKey();
                    int v = entry.getValue();
                    this.word.set(k);
                    this.frequency.set(v);
                    try {
                        context.write(this.word, this.frequency);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        }

        private void setupWordFreq(URI wordFreqFile) {
            try (BufferedReader br = new BufferedReader(new FileReader(wordFreqFile.toString()))) {
                String wordFreq;
                while ((wordFreq = br.readLine()) != null) {
                    String[] splitted = wordFreq.split(this.separator);
                    String word = splitted[0];
                    int freq = Integer.parseInt(splitted[1]);
                    this.templateWordFreqMap.put(word, freq);
                }
            } catch (IOException ioException) {
                System.err.println("Error reading " + wordFreqFile.toString() + " " + ioException);
            }
        }
    }

    public static class CommonWordsReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private final Text word = new Text();
        private final IntWritable frequency = new IntWritable();
        private Map<String, Integer> hashMap;

        @Override
        public void setup(Context context) {
            this.hashMap = new HashMap<>();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            int res = 0;
            for (IntWritable v: values) {
                res = v.get();
            }
            this.hashMap.put(key.toString(), res);
        }

        @Override
        public void cleanup(Context context) {
            this.hashMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEach((entry) -> {
                    String k = entry.getKey();
                    int v = entry.getValue();
                    this.word.set(k);
                    this.frequency.set(v);
                    try {
                        context.write(this.frequency, this.word);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        }
    }

    public static void main(String[] args) throws Exception {
        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path stopWords = new Path(args[2]);
        Path wordCountOutput1 = new Path("commonwords/wc_output/one");
        Path wordCountOutput2 = new Path("commonwords/wc_output/two");
        Path outputPath = new Path(args[3]);
        Path wc1 = new Path("commonwords/wc_output/one/part-r-00000");
        Path wc2 = new Path("commonwords/wc_output/two/part-r-00000");

        Configuration wordCount1Conf = new Configuration();
        Job wordCount1 = Job.getInstance(wordCount1Conf, "word count 1");
        wordCount1.setJarByClass(TopkCommonWords.class);
        wordCount1.setMapperClass(TokenizerMapper.class);
        wordCount1.setCombinerClass(WordCountReducer.class);
        wordCount1.setReducerClass(WordCountReducer.class);
        wordCount1.setOutputKeyClass(Text.class);
        wordCount1.setOutputValueClass(IntWritable.class);
        wordCount1.addCacheFile(stopWords.toUri());
        FileInputFormat.addInputPath(wordCount1, input1);
        FileOutputFormat.setOutputPath(wordCount1, wordCountOutput1);

        Configuration wordCount2Conf = new Configuration();
        Job wordCount2 = Job.getInstance(wordCount2Conf, "word count 2");
        wordCount2.setJarByClass(TopkCommonWords.class);
        wordCount2.setMapperClass(TokenizerMapper.class);
        wordCount2.setCombinerClass(WordCountReducer.class);
        wordCount2.setReducerClass(WordCountReducer.class);
        wordCount2.setOutputKeyClass(Text.class);
        wordCount2.setOutputValueClass(IntWritable.class);
        wordCount2.addCacheFile(stopWords.toUri());
        FileInputFormat.addInputPath(wordCount2, input2);
        FileOutputFormat.setOutputPath(wordCount2, wordCountOutput2);

        boolean wc1Done = wordCount1.waitForCompletion(true);
        boolean wc2Done = wordCount2.waitForCompletion(true);

        if (wc1Done && wc2Done) {
            Configuration commonWordsConf = new Configuration();
            Job commonWords = Job.getInstance(commonWordsConf, "common words");
            commonWords.setJarByClass(TopkCommonWords.class);
            commonWords.setMapperClass(CommonWordsMapper.class);
            commonWords.setReducerClass(CommonWordsReducer.class);
            commonWords.setOutputKeyClass(Text.class);
            commonWords.setOutputValueClass(IntWritable.class);
            commonWords.addCacheFile(wc1.toUri());
            FileInputFormat.addInputPath(commonWords, wc2);
            FileOutputFormat.setOutputPath(commonWords, outputPath);

            System.exit(commonWords.waitForCompletion(true) ? 0 : 1);
        }
        System.exit(1);
    }
}