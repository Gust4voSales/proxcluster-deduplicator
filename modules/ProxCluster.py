import pandas as pd
import polars as pl
from typing import Callable


class ProxCluster:
  def __init__(self, distanceFn: Callable[[dict, dict], list[float]], uID_column: str, threshold=0.4):
    """
    CustomKmeans initializer.

    Args:
      distanceFn ( (, ) -> float ): The distance function callback that receives the rows to compare and returns the distance.
      uID (str): The unique identifier to each item.
      threshold (float): The maximum distance threshold to identify a pair as duplicate. Defaults to 0.4.
    """
    self.df = pl.DataFrame()
    self.distanceFn = distanceFn
    self.uID_column = uID_column
    self.threshold = threshold
    self.comparisons = 0

    self.clusters: dict[any, list[dict]] | None = None

  def __get_centroid_by_uID(self, uID):
    # if clusters have been passed (incremental approach) get centroid from it
    if self.clusters != None:
      centroid = self.clusters[uID][0] # centroid is always the first item from the cluster
      return pl.DataFrame(centroid) 
   
    return self.df.filter(pl.col(self.uID_column) == uID) # find item from data
  
  def __custom_kmeans(self, centroids_uIDs: list):
    # getting the centroids rows by their uIDs
    centroids = pl.concat((self.__get_centroid_by_uID(centroid_uID) for centroid_uID in centroids_uIDs))
    # clusters (É um dicionário, a chave do dicionário é o uID do centroide, seu valor é um array de items)
    clusters: dict[any, list[dict]] = self.clusters if self.clusters!=None else {key: [] for key in centroids_uIDs} 

    for el in self.df.iter_rows(named=True):
      found_centroid = False
      for i, centroid in enumerate(centroids.iter_rows(named=True)):
        dist = self.distanceFn(el, centroid)
        self.comparisons += 1

        if (dist < self.threshold):
          min_centroid_uID = centroids_uIDs[i] # get the centroid uID from the index  
          clusters[min_centroid_uID].append(el) # Append the current element to that centroid
          found_centroid = True
          break
      
      if not found_centroid:
        new_centroid_uID = el[self.uID_column]
        centroids_uIDs.append(new_centroid_uID)        

        centroids = pl.concat([centroids, pl.DataFrame(el)] )  # add current element as centroid 
        clusters[new_centroid_uID] = [el] # Append the current element to that centroid

    return clusters
    
  def run(self, df: pd.DataFrame, clusters: dict | None = None):
    self.df = pl.DataFrame(df)
    self.clusters = None

    centroids_uIDs = []
    if (clusters == None):
      first_el_uID = self.df.select(pl.first(self.uID_column)).item()
      centroids_uIDs = [ first_el_uID ] # use first item as the first centroid      
    else: # incremental approach
      self.clusters = clusters
      centroids_uIDs = list(clusters.keys())
      
    clusters = self.__custom_kmeans(centroids_uIDs)

    return clusters
  

