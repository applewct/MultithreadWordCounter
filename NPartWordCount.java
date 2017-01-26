import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class NPartWordCount{
    public static void main(String args[])throws IOException{
        String fileName = args[0];
        String partNum = args[1];

        // open file and split into n part
        String fullFile = new String(Files.readAllBytes(Paths.get(fileName)));
        fullFile = fullFile.replaceAll("[^A-Za-z0-9\\s]","");
        fullFile = fullFile.toLowerCase();
        String[] fullWords = fullFile.split("\\s+");
        List<String> fullWordList = new ArrayList<String>(Arrays.asList(fullWords));


        int partitionSize = (int)Math.ceil((double)fullWordList.size()/ Integer.parseInt(partNum));
        List<List<String>> partitions = new LinkedList<List<String>>();
        for (int i = 0; i < fullWordList.size(); i += partitionSize) {
            partitions.add(fullWordList.subList(i,
            Math.min(i + partitionSize, fullWordList.size())));
        }

        // create list to hold all n partitioned result
        List< Map<String, Integer> > nWordCounts
        = new ArrayList< Map<String, Integer> >();

        // calculate time
        long startTime = System.currentTimeMillis();

        // create n thread to calculate counts
        ArrayList<ThreadCountOnePart> threadlist
        = new ArrayList<ThreadCountOnePart>();

        for ( int i = 0 ; i < partitions.size() ; i++ ) {
            ThreadCountOnePart t = new ThreadCountOnePart(partitions.get(i));
            threadlist.add(t);
            t.start();
        }

        // get result from thread
        for ( ThreadCountOnePart t: threadlist ) {
            try{
                t.join();
                nWordCounts.add( t.getWordCount() );
            } catch (Exception e){}
        }

        // time end
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Time Spend: " + totalTime + "ms");

        // combine result
        Map<String, Integer> totalWCount = new HashMap<String, Integer>();
        for ( Map<String, Integer> wcount: nWordCounts ){
            for (Map.Entry<String, Integer> entry : wcount.entrySet()){
                String iword = entry.getKey();
                Integer icount = entry.getValue();
                Integer count = totalWCount.get(iword);
                totalWCount.put(iword, count == null ? icount : count + icount);
            }
        }

        // print result
        for ( Map.Entry<String, Integer> entry : totalWCount.entrySet()){
            System.out.println(String.format("%30s %2s",
            entry.getKey(), entry.getValue()));
        }
    }
}

class ThreadCountOnePart extends Thread{
    private List<String> _part;
    private Map<String, Integer> _wordCount;

    public ThreadCountOnePart(List<String> part){
        this._part = part;
        this._wordCount = new HashMap<String, Integer>();
    }

    public Map<String, Integer> getWordCount(){
        return this._wordCount;
    }

    public void run(){
        for (String word: _part){
            //System.out.println(word);

            Integer count = _wordCount.get(word);
            _wordCount.put(word, count == null ? 1 : count + 1);
        }
    }
}
