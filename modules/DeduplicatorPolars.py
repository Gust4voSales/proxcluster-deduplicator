from modules.CustomKmeans import CustomKmeans
from modules.CustomKmeansPolars import CustomKmeansPolars
from modules.SoundexBlocking import SoundexBlocking
from modules.Evaluator import Evaluator

class DeduplicatorPolars:
  # TODO typing  
  def __init__(self, blocking_attr, distanceFn, uID, threshold):
    self.all_clusters = {} # TODO MEASURE TIME of updating this every loop (because this can be done later in the evaluation only)
    self.clusterized_blocks = [] # an array of clusters from each block
    self.clusterized_blocks_blocking_key_index_map = {} # mapping of blocking_key to the clusterized_block index 

    self.uID = uID
    self.blocker = SoundexBlocking(blocking_attr)
    self.customKmeans = CustomKmeansPolars(distanceFn, uID, threshold)

  def run(self, df):
    new_blocks = self.blocker.generate_blocks(df)   

    if len(self.clusterized_blocks) > 0: # incremental
      for block in new_blocks:
        # find index of the clusters block      
        block_blocking_key = block.iloc[0][self.blocker.blocking_key] # getting the blocking key of the current block

        # get the clusters that have the same blocking key 
        # we access them with clusterized_blocks index that we can get from clusterized_blocks_blocking_key_index_map
        clusters_block_index = self.clusterized_blocks_blocking_key_index_map.get(block_blocking_key)

        if (clusters_block_index != None): # found 
          clusters = self.customKmeans.run(block, self.clusterized_blocks[clusters_block_index])
        else:
          # new blocks, new cluster block
          self.clusterized_blocks_blocking_key_index_map[block_blocking_key] = len(self.clusterized_blocks) # add new mapping

          clusters = self.customKmeans.run(block)
          self.clusterized_blocks.append(clusters)
        
        self.all_clusters.update(clusters)

      return self.all_clusters

    # first time 
    for block in new_blocks:
      # new blocks, new cluster block
      block_blocking_key = block.iloc[0][self.blocker.blocking_key] # getting the blocking key of the current block
      self.clusterized_blocks_blocking_key_index_map[block_blocking_key] = len(self.clusterized_blocks) # add new mapping

      clusters = self.customKmeans.run(block)
      self.clusterized_blocks.append(clusters)
      self.all_clusters.update(clusters)
      
    return self.all_clusters
    
  def print_clusters_blocks(self):
    for i, block_clusters in enumerate(self.clusterized_blocks):
      print("CLUSTERS DO BLOCK ", i)
      for cluster_key in block_clusters.keys():
        print('>>>> CLUSTER: ' , cluster_key)
        for i, item in enumerate(block_clusters[cluster_key]):
          print('>>>> ',item['TID'], item['title'], item['artist'],  item['album'], )
        print()
      print('-'*100)

  def evaluate(self, golden_standard_array, debug=False):
    evaluator = Evaluator()
    evaluator.calculate_metrics(self.all_clusters, golden_standard_array, self.uID, debug)

    print(evaluator.get_report()) 
