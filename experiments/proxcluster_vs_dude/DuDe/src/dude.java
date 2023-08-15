import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

import de.hpi.fgis.dude.algorithm.Algorithm;
import de.hpi.fgis.dude.algorithm.duplicatedetection.SortedNeighborhoodMethod;
import de.hpi.fgis.dude.datasource.CSVSource;
import de.hpi.fgis.dude.output.DuDeOutput;
import de.hpi.fgis.dude.output.JsonOutput;
import de.hpi.fgis.dude.similarityfunction.contentbased.impl.simmetrics.LevenshteinDistanceFunction;
import de.hpi.fgis.dude.util.data.DuDeObjectPair;
import de.hpi.fgis.dude.util.sorting.sortingkey.SortingKey;
import de.hpi.fgis.dude.util.sorting.sortingkey.TextBasedSubkey;
import de.hpi.fgis.dude.similarityfunction.aggregators.Aggregator;
import de.hpi.fgis.dude.similarityfunction.aggregators.Average;

public class dude {
  
  public static void main(String[] args) throws Exception {
    // CSVSource source = new CSVSource("cddb", new File("../../../../datasets/cd_information", "cd.csv"));
    CSVSource source = new CSVSource("cddb", new File("./experiments/proxcluster_vs_dude/DuDe/src", "cd.csv"));
    source.enableHeader();

    // defines sub−keys used to generate the sorting key
    TextBasedSubkey titleSubkey = new TextBasedSubkey("title");
    // key generator uses sub−key selectors (order matters)
    SortingKey sortKey = new SortingKey();
    sortKey.addSubkey(titleSubkey);
    // new SNM algorithm with window size 20 and
    Algorithm algorithm = new SortedNeighborhoodMethod(sortKey, 20);
    algorithm.enableInMemoryProcessing();
    algorithm.addDataSource(source);

    LevenshteinDistanceFunction titleComp = new LevenshteinDistanceFunction("title");
    LevenshteinDistanceFunction artComp = new LevenshteinDistanceFunction("artist");
    LevenshteinDistanceFunction trackComp = new LevenshteinDistanceFunction("track01");

    Average comparator = new Average();
    
    // comparator.add(titleComp, 3);
    // comparator.add(artComp, 2);
    // comparator.add(trackComp, 1);
    comparator.add(titleComp);
    comparator.add(artComp);
    comparator.add(trackComp);

    //  DuDeOutput output = new JsonOutput(new FileOutputStream("./src/duplicates.json"));
    int comparisonsCounter = 0;
    ArrayList<Double> simis = new ArrayList<>();
    ArrayList<String[]> pairsList = new ArrayList<>();
    for (DuDeObjectPair pair: algorithm) {
      final double similarity = comparator.getSimilarity(pair);

      if (similarity > 0.75) {
        String[] pairIDs = {pair.getFirstElement().getAttributeValue("pk").toString(), pair.getSecondElement().getAttributeValue("pk").toString()};
        pairsList.add(pairIDs);
        comparisonsCounter++;
        simis.add(similarity);
        // System.out.println(similarity);
        // output.write(pair);
      }
    }
    System.out.println(comparisonsCounter);
    // System.out.println(pairsList.get(0)[0]);
    // System.out.println(pairsList.get(0)[1]);
    // System.out.println(simis.get(0));
    algorithm.cleanUp();
  }
}
