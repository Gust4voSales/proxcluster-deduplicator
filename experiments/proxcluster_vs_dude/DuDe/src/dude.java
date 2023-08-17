import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import de.hpi.fgis.dude.algorithm.Algorithm;
// import de.hpi.fgis.dude.algorithm.duplicatedetection.SortedNeighborhoodMethod;
// import de.hpi.fgis.dude.algorithm.duplicatedetection.NaiveDuplicateDetection;
import de.hpi.fgis.dude.algorithm.duplicatedetection.NaiveBlockingAlgorithm;
import de.hpi.fgis.dude.datasource.CSVSource;
import de.hpi.fgis.dude.similarityfunction.contentbased.impl.simmetrics.LevenshteinDistanceFunction;
import de.hpi.fgis.dude.util.data.DuDeObjectPair;
import de.hpi.fgis.dude.util.sorting.sortingkey.SortingKey;
import de.hpi.fgis.dude.util.sorting.sortingkey.TextBasedSubkey;
import de.hpi.fgis.dude.similarityfunction.aggregators.Average;


public class dude {
  
  public static void main(String[] args) throws Exception {
    CSVSource source = new CSVSource("cddb", new File("./experiments/proxcluster_vs_dude/DuDe/src", "cd.csv"));
    source.enableHeader();

    // defines sub−keys used to generate the sorting key
    TextBasedSubkey titleSubkey = new TextBasedSubkey("title");
    // key generator uses sub−key selectors (order matters)
    SortingKey sortKey = new SortingKey();
    sortKey.addSubkey(titleSubkey);
    // new SNM algorithm with window size 20 and
    // Algorithm algorithm = new SortedNeighborhoodMethod(sortKey, 20);
    Algorithm algorithm = new NaiveBlockingAlgorithm(sortKey);
    algorithm.enableInMemoryProcessing();
    algorithm.addDataSource(source);

    long startTime = System.currentTimeMillis();
    
    LevenshteinDistanceFunction titleComp = new LevenshteinDistanceFunction("title");
    LevenshteinDistanceFunction artComp = new LevenshteinDistanceFunction("artist");
    LevenshteinDistanceFunction trackComp = new LevenshteinDistanceFunction("track01");

    Average comparator = new Average();
    
    comparator.add(titleComp);
    comparator.add(artComp);
    comparator.add(trackComp);

    int comparisonsCounter = 0;
    ArrayList<Integer[]> pairsList = new ArrayList<>(); 
    for (DuDeObjectPair pair: algorithm) {
      final double similarity = comparator.getSimilarity(pair);
      comparisonsCounter++;

      if (similarity > 0.75) {
        int firstPairID = Integer.parseInt(pair.getFirstElement().getAttributeValue("pk").toString());
        int secondPairID = Integer.parseInt(pair.getSecondElement().getAttributeValue("pk").toString());
        Integer[] pairIDs = {firstPairID,secondPairID};
        pairsList.add(pairIDs);
      }
    }
    algorithm.cleanUp();
    long endTime = System.currentTimeMillis();

    System.out.println("Comparisons: " + comparisonsCounter);
    System.out.println("Time: " + (endTime - startTime));
    

    // Convert ArrayList to a JSON-formatted string
    String jsonString = arrayListToJson(pairsList);

    // Save the JSON string to a file
    try {
        FileWriter fileWriter = new FileWriter("./experiments/proxcluster_vs_dude/DuDe/dude_pairs.json");
        fileWriter.write(jsonString);
        fileWriter.close();
        System.out.println("ArrayList saved to JSON file successfully.");
    } catch (IOException e) {
        e.printStackTrace();
    }
  }

  // Convert ArrayList to JSON-formatted string manually
  private static String arrayListToJson(ArrayList<Integer[]> arrayList) {
    StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("[");
    
    for (Integer[] item : arrayList) {
        jsonBuilder.append("[");
        for (int i = 0; i < item.length; i++) {
            jsonBuilder.append(item[i]);
            if (i < item.length - 1) {
                jsonBuilder.append(",");
            }
        }
        jsonBuilder.append("]");
        
        if (item != arrayList.get(arrayList.size() - 1)) {
            jsonBuilder.append(",");
        }
    }
    
    jsonBuilder.append("]");
    return jsonBuilder.toString();
  }
}
