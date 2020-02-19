# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 20:45:45 2020

@author: @guhankumar
"""
 
import pandas as pd
import datetime

#scripts = ["ADANIPORTS", "BHARTIARTL","UPL", "AXISBANK", "HDFC", "ASIANPAINT","HINDUNILVR", "TITAN","TCS", "ZEEL", "TATASTEEL","HINDALCO","HDFCBANK", "ICICIBANK","INFRATEL","INDUSINDBK","HCLTECH","TECHM","INFY","DRREDDY"]
scripts = ["HEROMOTOCO","TCS","INFY","SBIN","HDFCBANK","BPCL","KOTAKBANK","UPL","TATASTEEL","HCLTECH","TECHM","SUNPHARMA","ASIANPAINT","LT","AXISBANK","RELIANCE","INDUSINDBK","ADANIPORTS","HDFC","GRASIM","BHARTIARTL","TITAN","HINDUNILVR","ICICIBANK","CIPLA","ZEEL","INFRATEL"]
scripts = ["HEROMOTOCO"]
def get_historical_data(script):
    alpha_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=NSE:{}&interval=15min&apikey=FIKNV2QYP8FRIOPF&outputsize=full&datatype=csv".format(script)
    hist_df = pd.read_csv(alpha_url, parse_dates=True)
    #print(hist_df)
    hist_df['date'] = pd.to_datetime(hist_df['timestamp'])
    hist_df['datetime'] = hist_df['date'].dt.tz_localize('US/Eastern').dt.tz_convert('Asia/Kolkata')
    hist_df['datetime'] = hist_df['datetime'].dt.tz_localize(tz=None)
    hist_df.drop(['timestamp','date'], axis=1, inplace=True)
    hist_df['date'] = hist_df['datetime'].dt.date
    hist_df['time'] = hist_df['datetime'].dt.time
    hist_df.set_index('datetime', inplace=True)
    #hist_df.set_index('time', inplace=True)
    return hist_df

def is_shooting_star(data):
    open = data['open']
    close = data['close']
    high = data['high']
    low = data['low'] 
    #RSI = data[4]
    #print("Open: {} High {} Low {} Close{} Time {}".format(open,high,low,close, time))
    #Lower Wick Calculation
    lower_wick = data['lower_wick']
    #Upper Wick Calculation
    upper_wick = data['upper_wick']
    if open < 500 :
      low_wick_per = 1
    elif open > 500:
      low_wick_per = 1.5
    #Body Calculation
    Body = data['body']
    #print("Low Wick: {} ;Body {} ; Upper Wick {}".format(lower_wick,Body,upper_wick))
    if open >= low and Body > 0:
      if lower_wick == 0 or lower_wick < low_wick_per:
          if upper_wick > (1.25 * Body):
              if lower_wick < Body:  
                 return True
              else:
                  return False
          else:
              return False
      else:
          return False
    else: 
          return False
     
def get_rsi_14(script):
    alpha_url = "https://www.alphavantage.co/query?function=RSI&symbol=NSE:{}&interval=15min&time_period=14&series_type=close&apikey=FIKNV2QYP8FRIOPF&datatype=csv".format(script) 
    rsi_df = pd.read_csv(alpha_url, parse_dates=True)
    rsi_df['date'] = pd.to_datetime(rsi_df['time'])
    rsi_df['datetime'] = rsi_df['date'].dt.tz_localize('US/Eastern').dt.tz_convert('Asia/Kolkata')
    rsi_df['datetime'] = rsi_df['datetime'].dt.tz_localize(tz=None)
    rsi_df.drop(['time','date'], axis=1, inplace=True)
    rsi_df.set_index('datetime', inplace=True)
    return rsi_df

def combine_ohlc_function(ohlc, rsi):
    combine_ohlc = pd.concat([ohlc, rsi], axis=1, sort=True)
    return combine_ohlc
    

if __name__ == "__main__" :
        scripts = ["HEROMOTOCO","TCS","INFY","SBIN","HDFCBANK","BPCL","KOTAKBANK","UPL","TATASTEEL","HCLTECH","TECHM","SUNPHARMA","ASIANPAINT","LT","AXISBANK","RELIANCE","INDUSINDBK","ADANIPORTS","HDFC","GRASIM","BHARTIARTL","TITAN","HINDUNILVR","ICICIBANK","CIPLA","ZEEL","INFRATEL"]
        enddate = datetime.datetime.now().date()
        #enddate = datetime.datetime(2020, 2 , 17).date()
        print("Processing the Data for date {}......".format(enddate))
        tickers_ohlc = {}
        for script in scripts:
            #print("Stock : {}".format(script))
            ohlc_data = get_historical_data(script)
            rsi_data = get_rsi_14(script)
            #print(rsi_data.tail(20))
            data_rsi = combine_ohlc_function(ohlc_data,rsi_data)
            #print(data_rsi.head(20))
            data = ohlc_data
            data_new = data_rsi.copy()
            data_new['upper_wick'] = data_new['high'] - data_new['close']
            data_new['lower_wick'] = data_new['open'] - data_new['low']
            data_new['body'] = data_new['open'] - data_new['close']
            
            data_new = data_new.assign(shooting_star=data_new.apply(is_shooting_star,axis=1))
            #data_91 = [data[]
            tickers_ohlc[script] = data_new
            print(tickers_ohlc)
           
            