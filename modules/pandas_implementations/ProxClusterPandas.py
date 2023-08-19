import pandas as pd
from typing import Callable

class ProxClusterPandas:
  # TODO proper typing
  def __init__(self, distanceFn: Callable[[], list[float]], uID: str, threshold=0.4):
    """
    ProxCluster initializer.

    Args:
      df (pandas.DataFrame): The dataframe object with the rows to deduplicate.
      distanceFn ( (pandas.Series, pandas.Series) -> float ): The distance function callback that receives the rows to compare and returns the distance.
      uID (str): The unique identifier to each item.
      threshold (float): The maximum distance threshold to identify a pair as duplicate. Defaults to 0.4.
    """
    self.df = pd.DataFrame()
    self.distanceFn = distanceFn
    self.uID = uID
    self.threshold = threshold

    self.clusters: dict[any, list[pd.Series]] | None = None

  def __get_centroid_by_uID(self, uID):
    # if clusters have been passed (incremental approach) get centroid from it
    if self.clusters != None:
      centroid_series = self.clusters[uID][0]
      return pd.DataFrame([centroid_series.to_list()], columns=centroid_series.index.to_list())
    
    return self.df.loc[self.df[self.uID] == uID] # find item from data
  
  def __custom_kmeans(self, centroids_uIDs: list):
    # getting the centroids rows by their uIDs
    centroids = pd.concat((self.__get_centroid_by_uID(centroid_uID) for centroid_uID in centroids_uIDs)).reset_index() 
    
    # clusters (É um dicionário, a chave do dicionário é o uID do centroide, seu valor é um array de items pd.Series)
    clusters: dict[any, list[pd.Series]] = self.clusters if self.clusters!=None else {key: [] for key in centroids_uIDs} 

    for _, el in self.df.iterrows():  
      found_centroid = False
      for i, (_, centroid) in enumerate(centroids.iterrows()):
        dist = self.distanceFn(el, centroid)

        if (dist < self.threshold):
          min_centroid_uID = centroids_uIDs[i] # get the centroid uID from the index  
          clusters[min_centroid_uID].append(el) # Append the current element to that centroid
      if not found_centroid:
        new_centroid_uID = el[self.uID]
        centroids_uIDs.append(new_centroid_uID)        

        centroids.loc[len(centroids)] = el # add current element as centroid 
        clusters[new_centroid_uID] = [el] # Append the current element to that centroid

    return clusters

  def run(self, df: pd.DataFrame, clusters: dict | None = None):
    """
      TODO
    Args:
      ...

    Returns:
      cluster: ...
    """
    
    self.df = df
    self.clusters = None

    centroids_uIDs = []
    if (clusters == None):
      first_el = self.df.iloc[0]
      centroids_uIDs = [ first_el[self.uID] ] # use first item as the first centroid      
    else: # incremental approach
      self.clusters = clusters
      centroids_uIDs = list(clusters.keys())
      
    clusters = self.__custom_kmeans(centroids_uIDs)

    return clusters
  