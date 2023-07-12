# DataMatching 179 

class Evaluator:
  def __init__(self):
    self.TP = 0 # True Positives: Classified as matches and that are true matches. 
    self.FP = 0 # False Positives: Classified as matches, but they are not true matches. 
    self.TN = -1111 # True Negatives: Classified as non-matches, and they are true non-matches. 
    self.FN = 0 # False Negatives: Classified as non-matches, but they are actually true matches. 
  
  '''
  Accuracy is mainly useful
  for situations where the classes are balanced, i.e. where the number of instances
  (record pairs in the case of data matching) are more or less the same for both
  classes (matches and non-matches).
  As was previously discussed, balanced classes are rare in data matching and deduplication classification, 
  in that the majority of record pairs corresponds to true
  non-matches (true negatives). As a result, the accuracy measure is not suitable
  to properly assess matching quality
  '''

  # TODO typing
  def __generate_clusters_pairs(self, cluster, uID):
    pairs = []
    for i in range(len(cluster)):
      for j in range(i+1, len(cluster)):
        pairs.append([ cluster[i][uID], cluster[j][uID] ])
    return pairs

  def __invert_pair(self, pair):
    return pair[::-1]

  def get_precision(self):
    try:
      return self.TP/(self.TP + self.FP)
    except ZeroDivisionError: 
      return 0

  def get_recall(self):
     try:
      return self.TP/(self.TP + self.FN)
     except ZeroDivisionError: 
      return 0
    
  def get_f_measure(self):
    precision = self.get_precision()
    recall = self.get_recall()

    try:
      return 2 * (precision*recall/(precision + recall))
    except ZeroDivisionError: 
      return 0
  
  # TODO: typing and TN (even though it won't be useful)
  # calculate TP, FP, TN, FN
  def calculate_metrics(self, clusters, golden_standard_array, uID, debug=False):
    all_pairs = []
    for cluster_key in clusters.keys():
      cluster_items_pairs = self.__generate_clusters_pairs(clusters[cluster_key], uID)
      
      if len(cluster_items_pairs)>0 and debug: print(cluster_items_pairs)

      all_pairs.extend(cluster_items_pairs)

      #  TP and FP computation
      for pair in cluster_items_pairs:
        if pair in golden_standard_array or self.__invert_pair(pair) in golden_standard_array:
          self.TP += 1
        else:
          self.FP += 1

    # FN computation
    for answer in golden_standard_array:
      if answer not in all_pairs and self.__invert_pair(answer) not in all_pairs:
        self.FN += 1
    
    # return self.TP, self.FP, self.TN, self.FN

  def get_report(self):
    precision = self.get_precision()
    recall = self.get_recall()
    f_measure = self.get_f_measure()

    return f'''~~ EVALUATION ~~
  Precision: {precision}
  Recall: {recall}
  F-measure: {f_measure}
'''
