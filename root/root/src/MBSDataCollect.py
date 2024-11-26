# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 08:08:52 2024

@author: Diego
"""

import os
import pandas as pd

class MBSDataCollect:
    
    def __init__(self) -> None:
        
        self.root_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.repo_path = os.path.abspath(os.path.join(self.root_path, os.pardir))
        self.data_path = os.path.join(self.repo_path, "data")
        self.raw_path  = os.path.join(self.data_path, "RawData")
        self.note_path = os.path.join(self.root_path, "notebooks")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        if os.path.exists(self.raw_path)  == False: os.makedirs(self.raw_path)
        if os.path.exists(self.note_path) == False: os.makedirs(self.note_path)
        
        self.tsy_futures  = ["FV", "US", "TY", "TU", "WN", "UXY"]
        self.mtg_tickers  = ["LUMS"]
        self.misc_tickers = ["SGIXTFIR", "MOVE", ".30CC105"]
        
        self.fut_path   = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\PXFront" 
        self.deliv_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\BondDeliverableRisk"
        self.mtge_path  = r"C:\Users\Diego\Desktop\app_prod\BBGData\credit_indices_data"
        #self.bbg_path   = r"C:\Users\Diego\Desktop\app_prod\BBGData\data"
        self.bbg_path = r"/Users/diegoalvarez/Desktop/BBGData/data"

    def _get_tsy_rtn(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(
                px_diff = lambda x: x.px.diff(),
                px_rtn  = lambda x: x.px.pct_change(),
                px_bps  = lambda x: x.px_diff / x.ctd_dur))
        
        return df_out

    def get_tsy_futures(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_path, "TreasuryFutures.parquet")
        try:
            
            if verbose == True: print("Trying to find Treasury Futures")
            df_tsy = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find Treasury Data, collecting it now")
            
            px_paths = [
                os.path.join(self.fut_path, file + ".parquet") 
                for file in self.tsy_futures]
            
            deliv_paths = [
                os.path.join(self.deliv_path, file + ".parquet")
                for file in self.tsy_futures]
            
            df_deliv = (pd.read_parquet(
                path = deliv_paths, engine = "pyarrow").
                pivot(index = ["date", "security"], columns = "variable", values = "value").
                rename(columns = {
                    "CONVENTIONAL_CTD_FORWARD_FRSK": "ctd_dur",
                    "FUT_EQV_CNVX_NOTL"            : "ctd_cnvx"}).
                dropna().
                reset_index())
    
            df_tsy = (pd.read_parquet(
                path = px_paths, engine = "pyarrow").
                rename(columns = {"PX_LAST": "px"}).
                merge(right = df_deliv, how = "inner", on = ["date", "security"]).
                assign(security = lambda x: x.security.str.split(" ").str[0]).
                groupby("security").
                apply(self._get_tsy_rtn).
                reset_index(drop = True).
                dropna())
        
            if verbose == True: print("Saving data\n")
            df_tsy.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_tsy
    
    def get_mtge_data(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_path, "MortgageIndex.parquet")
        try:
            
            if verbose == True: print("Trying to find Mortgage Indices Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
        
            if verbose == True: print("Couldn't find MBS data, getting it now")
            
            paths = ([
                os.path.join(self.mtge_path, file + ".parquet") 
                for file in self.mtg_tickers])
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]).
                pivot(index = "date", columns = "security", values = "value").
                rename(columns = {
                    "LUMSMD"  : "mod_dur",
                    "LUMSOAS" : "OAS",
                    "LUMSTRUU": "px"}).
                assign(
                    px_diff = lambda x: x.px.diff(),
                    px_rtn  = lambda x: x.px.pct_change(),
                    px_bps  = lambda x: x.px_diff / x.mod_dur).
                dropna())
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def get_misc(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_path, "MiscIndices.parquet")
        try:
            
            if verbose == True: print("Trying to find Misc Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find data collecting it now")
            
            paths = ([
                os.path.join(self.bbg_path, file + ".parquet")
                for file in self.misc_tickers])
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
       
def main() -> None: 
    
    MBSDataCollect().get_misc(verbose = True) 
    MBSDataCollect().get_mtge_data(verbose = True)
    MBSDataCollect().get_tsy_futures(verbose = True)
    
if __name__ == "__main__": main()