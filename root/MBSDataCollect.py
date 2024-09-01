# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 16:08:53 2024

@author: Diego
"""

import os
import pandas as pd

class MBSDataPrep:
    
    def __init__(self):
        
        self.fut_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\PXFront"
        self.mbs_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\credit_indices_data"
        self.deliverable_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\BondDeliverableRisk"
        
        self.root_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.data_path = os.path.join(self.root_path, "data")
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        
        self.tickers_path = os.path.join(os.getcwd(), "tickers.xlsx")
        self.df_tickers = pd.read_excel(io = self.tickers_path)
        
        
    def _get_futures_px(self) -> pd.DataFrame:
        
        tickers = (self.df_tickers.query(
            "security != 'LUMS'").
            security.
            to_list())
        
        tickers_path = [os.path.join(self.fut_path, ticker + ".parquet") for ticker in tickers]
        df_futures = (pd.read_parquet(
            tickers_path, engine = "pyarrow").
            assign(
                date = lambda x: pd.to_datetime(x.date).dt.date,
                security = lambda x: x.security.str.split(" ").str[0]))
        
        return df_futures
    
    def _get_duration(self) -> pd.DataFrame:
        
        tickers = (self.df_tickers.query(
            "security != 'LUMS'").
            security.
            to_list())
        
        tickers = [os.path.join(self.deliverable_path, ticker + ".parquet") for ticker in tickers]
        df_duration = (pd.read_parquet(
            path = tickers, engine = "pyarrow").
            assign(
                date = lambda x: pd.to_datetime(x.date).dt.date,
                security = lambda x: x.security.str.split(" ").str[0]).
            pivot(index = ["date", "security"], columns = "variable", values = "value").
            reset_index())
        
        return df_duration
    
    def get_futures_data(self) -> pd.DataFrame: 
        
        path = os.path.join(self.data_path, "tsy.parquet")
        try:
            
            df_out = (pd.read_parquet(
                path = path, engine = "pyarrow"))
            
        except: 
        
            df_out = (self._get_futures_px().merge(
                right = self._get_duration(), how = "inner", on = ["date", "security"]))
        
            df_out.to_parquet(path = path, engine = "pyarrow")
        
        return df_out
    
    def _get_mbs_data(self) -> pd.DataFrame: 
        
        endings = ["MD", "OAS", "TRUU"]
        
        securities = ["LUMS" + ending for ending in endings]
        path = os.path.join(self.mbs_path, "LUMS.parquet")
        
        df = (pd.read_parquet(
            path = path, engine = "pyarrow").
            drop(columns = ["variable"]).
            assign(security = lambda x: x.security.str.split(" ").str[0]).
            query("security == @securities").
            pivot(index = "date", columns = "security", values = "value").
            dropna())
        
        return df
    
    def get_mbs_data(self) -> pd.DataFrame: 
        
        path = os.path.join(self.data_path, "MBS.parquet")
        try:
            
            df_out = pd.read_parquet(path = path, engine = "pyarrow")
            
        except: 
            
            df_out = self._get_mbs_data()
            df_out.to_parquet(path = path, engine = "pyarrow")
            
        return df_out
            
            
def main():
    
    MBSDataPrep().get_futures_data()
    MBSDataPrep().get_mbs_data()
    
#if __name__ == "__main__": main()