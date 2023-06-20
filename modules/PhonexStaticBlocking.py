import pandas as pd
from phonex import phonex

class PhonexStaticBlocking:
  block_key = 'block_key'

  def __init__(self, df: pd.DataFrame, key: str, MAX_BLOCK_SIZE: int):
    self.df = df
    self.blocks: list[pd.DataFrame] = []
    self.key = key
    self.MAX_BLOCK_SIZE = MAX_BLOCK_SIZE

  def __calculate_phonex(self, string: str):
    string = ''.join(filter(str.isalpha, string))
    return phonex(string)

  def __add_phonex_key(self):
    # calculate phonex to all key (column passed) values and assign it to the PhonexStaticBlocking.block_key column
    self.df[PhonexStaticBlocking.block_key] = self.df[self.key].apply(self.__calculate_phonex) 

    self.df = self.df.sort_values(PhonexStaticBlocking.block_key, ascending=True) # sort by title_phonex 

  # def divide_base_into_blocks(self,start_range, end_range):
  def __divide_base_into_blocks(self):
    num_blocks = 0
    block_size = self.MAX_BLOCK_SIZE
    if(len(self.df) % block_size==0):
      num_blocks = len(self.df) // block_size
    else:
      num_blocks = len(self.df) // block_size + 1

    # divide the dataframe into blocks of block_size rows each
    self.blocks = [self.df[i:i+block_size] for i in range(0, num_blocks*block_size, block_size)]
        
  def get_blocks(self):
    self.__add_phonex_key()
    self.__divide_base_into_blocks()

    return self.blocks
  

