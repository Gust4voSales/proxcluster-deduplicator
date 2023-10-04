from modules.pandas_implementations.ProxClusterPandas import ProxClusterPandas
from modules.SoundexBlocking import SoundexBlocking
from modules.Evaluator import Evaluator

class DeduplicatorPandas:
 # TODO typing  
  def __init__(self, blocking_attr, distanceFn, uID, threshold):
    self.all_clusters = {} 
    self.bkv_clusters_map = {} # mapping of blocking_key value to the clusters with that bkv

    self.uID = uID
    self.blocker = SoundexBlocking(blocking_attr)
    self.prox_cluster = ProxClusterPandas(distanceFn, uID, threshold)

  def run(self, df):
    new_blocks = self.blocker.generate_blocks(df)
    
    for block in new_blocks:
      blocking_key_value = block.iloc[0][self.blocker.blocking_key] # blocking key valeu from the current bloc
      clusterized_block = self.bkv_clusters_map.get(blocking_key_value)
      
      if clusterized_block is not None:  # found clusters with same blocking key valeu, do Incrementally
        clusters = self.prox_cluster.run(block, clusterized_block)
      else: # new blocking key valeu
        clusters = self.prox_cluster.run(block)
      
      self.bkv_clusters_map[blocking_key_value] = clusters
      self.all_clusters.update(clusters)

    return self.all_clusters

  def print_clusters_blocks(self):
    for key, block_clusters in self.bkv_clusters_map.items():
      print("CLUSTERS DO BLOCK ", key)
      for cluster_key in block_clusters.keys():
        print('>>>> CLUSTER: ' , cluster_key)
        for i, item in enumerate(block_clusters[cluster_key]):
          print('>>>> ',item)
        print()
      print('-'*100)

  def evaluate(self, golden_standard_array, debug=False):
    evaluator = Evaluator()
    evaluator.calculate_metrics(self.all_clusters, golden_standard_array, self.uID, debug)

    print(evaluator.get_report()) 
