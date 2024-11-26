# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 09:36:33 2024

@author: Diego
"""

import os
import numpy as np
import pandas as pd
from   MBSDataCollect import MBSDataCollect

class DataPrep(MBSDataCollect):
    
    def __init__(self) -> None:
        
        super().__init__()
        
        self.signal_path = os.path.join(self.data_path, "SignalData")
        if os.path.exists(self.signal_path) == False: os.makedirs(self.signal_path)
        
        self.window = {
            "short_window": 10,
            "long_window" : 100}
        
        self.vol_window = 30
        
    def _get_signal(self, df: pd.DataFrame, short_window: int, long_window: int) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(
                short_mean = lambda x: x.px.ewm(span = short_window, adjust = False).mean(),
                long_mean  = lambda x: x.px.ewm(span = long_window, adjust = False).mean(),
                signal     = lambda x: (x.short_mean - x.long_mean) / x.px,
                lag_signal = lambda x: x.signal.shift(),
                signal_bps = lambda x: np.sign(x.lag_signal) * x.px_bps,
                signal_rtn = lambda x: np.sign(x.lag_signal) * x.px_rtn))
        
        return df_out
        
    def get_tsy_signal(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.signal_path, "TreasurySignals.parquet")
        try:
            
            if verbose == True: print("Trying to find Treasury Signals")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
        
            if verbose == True: print("Couldn't find data, getting Treasury Signals")
            df_out = (self.get_tsy_futures().groupby(
                "security").
                apply(
                    self._get_signal, 
                    self.window["short_window"], 
                    self.window["long_window"]).
                reset_index(drop = True).
                dropna())
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def _get_vol(self, df: pd.DataFrame, window: int) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(
                roll_vol = lambda x: x.signal_rtn.rolling(window = window).std(),
                lag_vol  = lambda x: x.roll_vol.shift(),
                inv_vol  = lambda x: 1 / x.lag_vol).
            dropna())
        
        return df_out
    
    def get_erc_weighting(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.signal_path, "TreasuryERCWeighting.parquet")
        try:
            
            if verbose == True: print("Trying to find Treasury ERC weighting")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find ERC Weighting, getting it")
            
            df_inv = (self.get_tsy_signal().groupby(
                "security").
                apply(self._get_vol, self.vol_window).
                reset_index(drop = True))
            
            df_cum_vol = (df_inv[
                ["date", "inv_vol"]].
                groupby("date").
                agg("sum").
                rename(columns = {"inv_vol": "cum_vol"}))
            
            df_out = (df_inv.merge(
                right = df_cum_vol, how = "inner", on = ["date"]).
                assign(weight = lambda x: x.inv_vol / x.cum_vol))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def replicate_sg_index(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.signal_path, "IndexReplication.parquet")
        try:
            
            if verbose == True: print("Trying to find Index Replication Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find Index Replication Data, collecting now")
            
            df_avg = (self.get_tsy_signal()[
                ["date", "signal_rtn"]].
                groupby("date").
                agg("mean").
                rename(columns = {"signal_rtn": "AvgWeight"}))
            
            df_erc = (self.get_erc_weighting().assign(
                weight_rtn = lambda x: x.weight * x.signal_rtn)
                [["date", "weight_rtn"]].
                groupby("date").
                agg("sum").
                rename(columns = {"weight_rtn": "EqualVolRisk"}))
            
            df_sg = (self.get_misc().query(
                "security == 'SGIXTFIR'").
                drop(columns = ["security"]).
                rename(columns = {"value": "SocGenTrend"}))
            
            df_out = (df_avg.merge(
                right = df_erc, how = "inner", on = ["date"]).
                merge(right = df_sg, how = "inner", on = ["date"]).
                set_index(["date", "SocGenTrend"]).
                apply(lambda x: np.cumprod(1 + x) - 1).
                reset_index().
                set_index("date"))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out

def main() -> None:
        
    DataPrep().get_tsy_signal(verbose = True)
    DataPrep().get_erc_weighting(verbose = True)
    DataPrep().replicate_sg_index(verbose = True)
    
if __name__ == "__main__": main()