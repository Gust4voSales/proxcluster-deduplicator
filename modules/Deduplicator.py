from modules.CustomKmeans import CustomKmeans
from modules.SoundexBlocking import SoundexBlocking
from modules.Evaluator import Evaluator

class Deduplicator:
 # TODO typing  
  def __init__(self, blocking_attr, distanceFn, uID, threshold):
    self.all_clusters = {} # TODO MEASURE TIME of updating this every loop (because this can be done later in the evaluation only)
    self.bkv_clusters_map = {} # mapping of blocking_key value to the clusters with that bkv

    self.uID = uID
    self.blocker = SoundexBlocking(blocking_attr)
    self.customKmeans = CustomKmeans(distanceFn, uID, threshold)

  def run(self, df):
    new_blocks = self.blocker.generate_blocks(df)
    
    for block in new_blocks:
      blocking_key_value = block.iloc[0][self.blocker.blocking_key] # blocking key valeu from the current bloc
      clusterized_block = self.bkv_clusters_map.get(blocking_key_value)
      
      if clusterized_block is not None:  # found clusters with same blocking key valeu, do Incrementally
        clusters = self.customKmeans.run(block, clusterized_block)
        self.bkv_clusters_map[blocking_key_value] = clusters
      else: # new blocking key valeu
        clusters = self.customKmeans.run(block)
        self.bkv_clusters_map[blocking_key_value] = clusters
      
      self.all_clusters.update(clusters)

    return self.all_clusters

  def print_clusters_blocks(self):
    for key, block_clusters in self.bkv_clusters_map.items():
      print("CLUSTERS DO BLOCK ", key)
      for cluster_key in block_clusters.keys():
        print('>>>> CLUSTER: ' , cluster_key)
        for i, item in enumerate(block_clusters[cluster_key]):
          print('>>>> ',item['pk'], item['title'], item['artist'],  )
        print()
      print('-'*100)

  def evaluate(self, golden_standard_array, debug=False):
    evaluator = Evaluator()
    evaluator.calculate_metrics(self.all_clusters, golden_standard_array, self.uID, debug)

    print(evaluator.get_report()) 
