import re 
import Levenshtein as lev

def remove_non_alphanum(string: str):
  return re.sub(r'\W+', '', string)

def get_normalized_levenshtein_dist(string1: str, string2: str):
  x = remove_non_alphanum(string1).lower()
  y = remove_non_alphanum(string2).lower()
  
  dist = lev.distance(x, y) 

  max_len = max(len(x), len(y))
  if (max_len == 0):
    return 0
  normalized = (max_len-dist) / max_len 
  normalized_dist = 1-normalized
  return normalized_dist