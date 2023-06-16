import pandas as pd
import numpy as np
from typing import Callable

class CustomKmeans:
  def __init__(self, df: pd.DataFrame, distanceFn: Callable[[], list[float]], uID: str, threshold=0.4):
    """
    CustomKmeans initializer.

    Args:
      df (pandas.DataFrame): The dataframe object with the rows to deduplicate.
      distanceFn ( (pandas.Series, pandas.Series) -> float ): The distance function callback that receives the rows to compare and returns the distance.
      uID (str): The unique identifier to each item.
      threshold (float): The maximum distance threshold to identify a pair as duplicate. Defaults to 0.4.
    """

    self.df = df
    self.distanceFn = distanceFn
    self.uID = uID
    self.threshold = threshold

  def __get_item_by_uID(self, uID):
    item = self.df.loc[self.df[self.uID] == uID]
    return item
  
  def __get_distance_to_all_centroids(self, el: pd.Series, centroids: pd.DataFrame):  
    distances = []
    for _, row in centroids.iterrows():
      distances.append(self.distanceFn(el, row))
      
    return np.array(distances)

  def run(self):
    """
      TODO
    Args:
      ...

    Returns:
      cluster: ...
    """
    first_el = self.df.iloc[0]
    centroids_uIDs = [ first_el[self.uID] ] # use first item as the first centroid
  
    # clusters (É um dicionário, a chave do dicionário é o uID do centroide, seu valor é um array de items)
    clusters = {key: [] for key in centroids_uIDs} # TODO stop using centroid uID as key

    centroids = pd.DataFrame(self.__get_item_by_uID(first_el[self.uID])) # getting the centroids rows by their uIDs
    clusters_dists = {key: [] for key in centroids_uIDs} # use first item as the first centroid

    for index, el in self.df.iterrows(): 
      dists = self.__get_distance_to_all_centroids(el, centroids) # calculating the distance from the current element to the centroids, returns --> [distance_to_1st_cent, distance_to_2nd_cent]
      centroid_index_with_min_dist = np.argmin(dists)# get the index of the centroid with the minimum distance to the current element

      if (dists[centroid_index_with_min_dist] < self.threshold):
        min_centroid_uID = centroids_uIDs[centroid_index_with_min_dist] # get the centroid uID from the index  
        clusters[min_centroid_uID].append(el) # Append the current element to that centroid
        
        # TODO remove later 
        # debug distances
        if index>0:
          clusters_dists[min_centroid_uID].append(dists[centroid_index_with_min_dist])
      else:
        new_centroid_uID = el[self.uID]
        centroids_uIDs.append(new_centroid_uID)

        centroids.loc[index] = el # add current element as centroid 
        clusters[new_centroid_uID] = [el] # Append the current element to that centroid

        # TODO remove later 
        # debug distances
        clusters_dists[new_centroid_uID] =  [0]

    
    return clusters, clusters_dists