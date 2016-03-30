package DCacheJoin;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class DCacheJoinDictionaryMapper  extends Mapper<Text, Text, Text, Text> {
 	String fileName=null, language=null;
 	String translation="",pos="",englishText="",latinText="";
 	private Text word = new Text();
 	private Text one = new Text();
	   public Map<String, String> translations = new HashMap<String, String>();
	   
	   
	   public void setup(Context context) throws IOException, InterruptedException{
		// TODO: determine the name of the additional language based on the file name 
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
	       language = fileSplit.getPath().getName();
		   Configuration conf = context.getConfiguration();
	       Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
	       fileName = localFiles[0].toString();
	       fileName = fileName.replace(".txt", "");
	       BufferedReader bf = new BufferedReader(new FileReader(language));
	    // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1
	       while(bf.readLine()!=null){
	    	   String record = bf.readLine();
	    	   String[] languages = record.split(" ");
	    	   if(languages.length >1){
	    	   String englishWord = languages[0];
	    	   String pos = languages[1];
	    	   translations.put(englishWord+pos,record);
	    	   }
	       }
	    	bf.close();
	   }

	   public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
		   
		   BufferedReader br = new BufferedReader(new FileReader(fileName));
		   if (value.toString().endsWith("]")) {
		
			  
	    		 StringTokenizer itr = new StringTokenizer(value.toString(),".*[.*]");
	       		 	while (itr.hasMoreTokens())
	    		 	{
	       		 	String[] englishWord = null;
	    		 		if(itr.countTokens()==3){
	    		 		 englishText = itr.nextToken();
	        			 translation = itr.nextToken();
	        			 englishWord= englishText.split(" ");
	        			 if(translations.containsKey(englishWord[0])){
	        				 String latinText = "|"+fileName+":"+englishWord[1];
	        			 }
	        			 latinText = "|"+fileName+":N/A";
	    		 		}
	    		  word.set(englishWord[1]);
	    		  one.set(translation+latinText);
	        	  context.write(word, one);
	       }
  	       
		   
	    }

	   }
}
