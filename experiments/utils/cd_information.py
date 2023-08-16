import numpy as np
from modules.utils.NormalizedLevenshtein import get_normalized_levenshtein_dist
STRING_COLUMNS = ['title','artist','track01']

# replace NaN with '' on STRING_COLUMNS
def clean_strings(df):
  df[STRING_COLUMNS] = df[STRING_COLUMNS].replace(np.nan, '')
  return df

UNUSED_COLUMNS = ['id',	"category","genre","cdextra","year","track02","track03","track04","track05","track06","track07","track08","track09","track10","track11","track12","track13","track14","track15","track16","track17","track18","track19","track20","track21","track22","track23","track24","track25","track26","track27","track28","track29","track30","track31","track32","track33","track34","track35","track36","track37","track38","track39","track40","track41","track42","track43","track44","track45","track46","track47","track48","track49","track50","track51","track52","track53","track54","track55","track56","track57","track58","track59","track60","track61","track62","track63","track64","track65","track66","track67","track68","track69","track70","track71","track72","track73","track74","track75","track76","track77","track78","track79","track80","track81","track82","track83","track84","track85","track86","track87","track88","track89","track90","track91","track92","track93","track94","track95","track96","track97","track98","track99"]
def remove_unused_columns(df):
  return df.drop(columns = UNUSED_COLUMNS)

# a single function that calls all the above clean functions
def clean_db(df):
  df = remove_unused_columns(df)
  df = clean_strings(df)
  return df


# DISTANCE FUNCTION
title_w = 1
artist_w = 1
track1_w =1
def distance(item: dict, item2: dict): 
  w_sum = 0

  title_dist = 0
  artist_dist = 0
  track1_dist = 0
  
  if (item['title'] and item2['title']):
    title_dist = get_normalized_levenshtein_dist(item['title'], item2['title'])
    w_sum += title_w

  if (item['artist'] and item2['artist']):
    artist_dist = get_normalized_levenshtein_dist(item['artist'], item2['artist'])
    w_sum += artist_w

  if (item['track01'] and item2['track01']):
    track1_dist = get_normalized_levenshtein_dist(item['track01'], item2['track01'])
    w_sum += track1_w

  dist = ( (title_dist*title_w) + (artist_dist*artist_w) + (track1_dist*track1_w) ) / w_sum
  return dist
