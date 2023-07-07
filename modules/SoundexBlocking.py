import pandas as pd
import unicodedata
import fuzzy

soundex = fuzzy.Soundex(4)

class SoundexBlocking:
  blocking_key = 'blocking_key'

  def __init__(self, key: str):
    self.df: pd.DataFrame = None
    self.key = key

  def __calculate_soundex(self, string: str):
    string = ''.join(filter(str.isalpha, string))
    # only ascii chars allowed to soundex lib 
    string = unicodedata.normalize('NFKD', str(string)).encode('ascii', 'ignore').decode("utf-8") 
    return soundex(string)
 
  def __add_soundex_key(self):
    # calculate soundex to all key (column passed) values and assign it to the SoundexBlocking.blocking_key column
    self.df[SoundexBlocking.blocking_key] = self.df[self.key].apply(self.__calculate_soundex) 

  def __divide_base_into_blocks(self):
    # Group the DataFrame based on blocking_key
    groups = self.df.groupby(SoundexBlocking.blocking_key)

    blocks: list[pd.DataFrame] = []

    # Iterate over the groups and extract the rows
    for _, group in groups:  
      blocks.append(group)
    
    return blocks
  
  def merge_blocks(self, bigger_blocks, incremental_blocks):
    for block in incremental_blocks:
      item = block.iloc[0]

      found_block = False
      for i, block_to_compare in enumerate(bigger_blocks):
        if block_to_compare.iloc[0][SoundexBlocking.blocking_key] == item[SoundexBlocking.blocking_key]:
          bigger_blocks[i] = pd.concat([bigger_blocks[i], block], axis=0).reset_index(drop=True)
          found_block = True
          break
      
      if not found_block:
        bigger_blocks.append(block)
    
    return bigger_blocks
    
  def get_row_block_index(self, row: pd.Series, blocks):
    for i, block in enumerate(blocks):
      item = block.iloc[0]
      if (item[SoundexBlocking.blocking_key] == row[SoundexBlocking.blocking_key]):
        return i
    return -1 # not found

  def generate_blocks(self, df: pd.DataFrame):
    self.df = df
    
    self.__add_soundex_key()
    blocks = self.__divide_base_into_blocks()
    
    return blocks
  


