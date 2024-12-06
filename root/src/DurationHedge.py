#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 00:38:21 2024

@author: diegoalvarez
"""
import os
import pandas as pd
from   MBSDataPrep import DataPrep

class DurationHedge(DataPrep):
    
    def __init__(self) -> None:
        
        super().__init__()
        
    def _duration_hedge(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.assign(
            tmp1           = lambda x: x.mbs_dur / (x.tsy_dur + x.mbs_dur),
            mbs_weight     = lambda x: x.tsy_dur * x.tmp1 / x.mbs_dur,
            tsy_weight     = lambda x: 1 - x.mbs_weight,
            lag_mbs_weight = lambda x: x.mbs_weight.shift(),
            lag_tsy_weight = lambda x: x.tsy_weight.shift()).
            drop(columns = ["tmp1"]))
        
        return df_out
        
    def duration_hedge(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.signal_path, "DurationHedge.parquet")
        try:
            
            if verbose == True: print("Trying to find Duration Hedge")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
            
            if verbose == True: print("Couldn't find data, now collecting it")
            df_tsy = (self.get_tsy_futures()[
                ["date", "security", "ctd_dur"]].
                rename(columns = {
                    "security": "tsy_fut",
                    "ctd_dur" : "tsy_dur"}))
            
            df_out = (self.get_mtge_data()[
                ["mod_dur"]].
                rename(columns = {"mod_dur": "mbs_dur"}).
                reset_index().
                merge(right = df_tsy, how = "inner", on = ["date"]).
                groupby("tsy_fut").
                apply(self._duration_hedge).
                reset_index(drop = True).
                dropna())
            
            if verbose == True: print("Saving Duration Hedge")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def _min_mse(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        return df.query("mse == mse.min()")
    
    def get_matched_future(self, verbose: bool = False) -> pd.DataFrame:
        
        file_path = os.path.join(self.signal_path, "MatchedTreasury")
        try:
            
            if verbose == True: print("Trying to find Matched Treasury")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find data, now collecting it")
            
            df_tsy = (self.get_tsy_futures()[
                ["date", "security", "ctd_dur"]].
                rename(columns = {
                    "security": "tsy_fut",
                    "ctd_dur" : "tsy_dur"}))
            
            df_out = (self.get_mtge_data()[
                ["mod_dur"]].
                rename(columns = {"mod_dur": "mbs_dur"}).
                merge(right = df_tsy, how = "inner", on = ["date"]).
                assign(mse = lambda x: (x.mbs_dur - x.tsy_dur) ** 2).
                groupby("date").
                apply(self._min_mse).
                reset_index(drop = True).
                sort_values("date").
                assign(lag_tsy = lambda x: x.tsy_fut.shift()).
                dropna())
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
 
def main() -> None:   
 
    DurationHedge().get_matched_future(verbose = True)
    DurationHedge().duration_hedge(verbose = True)
    
if __name__ == "__main__": main()