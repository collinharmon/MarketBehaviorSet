import alpaca_trade_api as alpaca_api
from multiprocessing import Process, Queue, Manager
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
import mysql.connector
import os
import math
import datetime
import dateutil.parser
import numpy as np
import os
import requests
import ntpath
import threading

import configparser
import json


MYSQL_ADD_USER = "INSERT INTO users (discord_id) VALUES (%s)" 
MYSQL_SELECT_USER = "SELECT user_id FROM users WHERE discord_id = %s"

MYSQL_SELECT_PORTFOLIO = "SELECT index_id FROM indexes WHERE index_name = %s and user_id = %s"
MYSQL_CREATE_PORTFOLIO = "INSERT INTO indexes (index_name, user_id) VALUES (%s, %s)"

MYSQL_SELECT_STOCK = "SELECT stock_id FROM stocks WHERE ticker = %s and index_id = %s"
MYSQL_ADD_STOCK = "INSERT INTO stocks (ticker, index_id) VALUES (%s, %s)"

MYSQL_SELECT_BARSET = "SELECT barset_id FROM barsets WHERE ticker = %s and day = %s"
MYSQL_ADD_BARSET = "INSERT INTO barsets (ticker, day, open, close, high, low, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"

MYSQL_SELECT_INDEX_NAMES = "SELECT index_name FROM indexes WHERE user_id = %s"

TICKERS_ONLY = 1
TICKERS_WEIGHT = 2
TICKERS_SHARES = 3

MINIMUM_TICKER_LENGTH = 1
MAXIMUM_TICKER_LENGTH = 5


class MarketApi:
  """
  A custom Stock Market API for the MarketBehaviorSet
  """
  def __init__(self):
    super().__init__()
    self.set_env_vars()
    self.alpaca_api = alpaca_api.REST()
    self.market_db = self.MarketDatabaseHelper()
    self.mutex = threading.Lock()


  def create_portfolio(self, user_id, tickers_data, name):
    """
    Parameters  - user_id:      str of Discord user ID
                  tickers_data: str buffer containing tickers delimited by `\n`
                  name:         str specifying the name of the portfolio to be created

    Return      - str containing status message of portfolio creation attempt

    Description - This function will attempt to parse a str buffer presumed to contain the ticker data which defines the portfolio. If successful
                  retrieve the requesting Discord user's (per user_id) profile in the database (or create if one does not exist).  Then use the
                  MySQL database interface object, `self.market_db` (MarketDatabaseHelper), to insert the portfolio into the database for the Discord user. 

    *Raised errors to be handled by the function which calls this function
    """
    try:
      (tickers, format) = self.parse_tickers(tickers_data)
    except Exception as e:
      raise Exception("There was an issue parsing the tickers data: %s" % str(e))
    
    (user_db_id, exists) = self.market_db.get_user_id(user_id)

    rval = self.market_db.insert_portfolio(name, tickers, format, user_db_id)

    if rval == -1:
      return "There was a database error. Contact admin."
    elif rval == 0:
      return "Portfolio `%s` already exists for user <@%d>." % (name, user_id)
    else:
      return "Portfolio `%s` was successfully created for user <@%d>." % (name, user_id)
  
  def parse_tickers(self, text):
    """
    Parameters  - text: str buffer containing tickers delimited by `\n`

    Return      - ([] of stock tickers, format code of .csv file provided)

    Description - given a CSV of tickers, parse and store in an array to be returned. Determine .csv format by identifying header type.
    
    Format Codes (See README for more info):
      - TICKERS_ONLY:   1
      - TICKERS_WEIGHT: 2
      - TICKERS_SHARES: 3
    """
    tickers = []
    format = 0

    if not text:
      #provide just white space rather than 0kb file
      raise ValueError("csv provided is empty.")

    lines = text.split("\n")

    csv_header = lines.pop(0)
    #in case the header is preceded by newlines..
    while csv_header.isspace() and len(lines) > 0:
      csv_header = lines.pop(0)
    if csv_header.isspace():
      raise ValueError("csv header is blank for:")

    try:
      format = self.get_csv_format(csv_header)
    except ValueError as ve:
      raise ve

    line_number = 2 #header was line 1 so we start at 2
    for line in lines:
      line_number = line_number + 1
      if line.isspace() or line == "":
        #skip blank lines
        continue
      try:
        tickers.append(self.tokenize_string(line, line_number-1, format))
      except ValueError as ve:
        raise ve

    return (tickers, format)

  def get_csv_format(self, header):
    """
    Parameters  - header: str containing the header of a .csv file containing a portfolio definition

    Return      - int Format Code (see below)

    Description - given the header line of a .csv file containing a portfolio definition, determine format
    
    Expects 1 of the 3 following formats:
      TICKERS_ONLY:   ticker (code: 1)
      TICKERS_WEIGHT: ticker, weight (in decimal) (code: 2) 
      TICKERS_SHARES: ticker, share_count, avg_price (in decimal) (code: 3)
    """
    tokens = header.split(",")

    num_columns = len(tokens)
    if num_columns != 1 and num_columns != 3:
      error_msg = ""
      if tokens.pop().isspace():
        error_msg = "csv header has invalid number of columns. Make sure your last column doesn't end with a comma! For:"
      else:
        error_msg = "csv header invalid. Invalid number of columns in header for:"
      raise ValueError(error_msg)

    if tokens[0].strip() != "ticker":
      print("the token %s" % tokens[0])
      print("the header %s" % header)
      raise ValueError("csv header invalid for format %s. String in first column of header is not equal to \"ticker\" for:" % str(num_columns))
    
    if num_columns == 1:
      return TICKERS_ONLY
    else:
      if tokens[1].strip() == "shares" and tokens[2].strip() == "avg_cost":
        return TICKERS_SHARES
      elif tokens[1].strip() == "weight_initial" and tokens[2].strip() == "weight_now":
        return TICKERS_WEIGHT
      else:
        raise ValueError("Format Error: Invalid column keys")

  def tokenize_string(self, line, line_number, format):
    """
    Parameters  - line:        str containing a single line of a .csv defining a portfolio
                  line_number: int which indicates line number of provided line (not used in logic, used for useful error messages)
                  format:      the format of the .csv file currently being processed

    Return      - Tuple containing ticker and its weight (if format 2 or 3)

    Description - given line containing a ticker, parse the ticker string and call `self.validate_ticker` to validate the ticker exists on the market (done by querying via AlpacaAPI).
                  If valid return a tuple containing the str defining the ticker. If format 2 or 3, return tuple additionally containing the weight information.
    """
    tokens = line.strip().split(",")
    if format == TICKERS_ONLY:
      format_columns = 1
    else:
      format_columns = 3

    if len(tokens) != format_columns:
      raise ValueError("Format %s, invalid format on line %d. Be sure lines don't end with a comma. For:" % (format, line_number))

    token_ticker = tokens[0].strip()
    token_ticker = self.validate_ticker(token_ticker)
    if token_ticker is None:
      raise ValueError("Ticker string, %s on line %d is invalid for:" % (token_ticker, line_number))

    if format == TICKERS_ONLY:
      return (token_ticker,)
    else:
      try:
        token_column_two = float(tokens[1].strip())
      except:
        raise ValueError("Format %d error. Value, `%s`, on line %d, column %d is not a valid number." % (format, tokens[1].strip(), line_number, 2))

      try:
        token_column_three = float(tokens[2].strip())
      except:
        raise ValueError("Format %d error. Value, `%s`, on line %d, column %d is not a valid number." % (format, tokens[2].strip(), line_number, 3))

      return (token_ticker, token_column_two, token_column_three)


  def get_return(self, tickers, start_time, end_time):
    """
    Parameters  - tickers:    list of type str containing tickers
                  start_time: str of start time
                  end_time:   str of end time
    
    Note: The formats supported for the provided times are as follows:
            - ISO
            - YYYY/MM/DD
            - YYYY\MM\DD
            - YYYY-MM-DD

    Return      - dictionary where k=ticker_name v=ticker return with respect start/end time delta 

    Description - Provided a list of tickers and a timeframe, obtain the return of each provided ticker with
                  respect to the user-defined timeframe.
    """

    try:
      datetime_start_time = self.validate_timestamp(start_time)
    except ValueError as ve:
      raise ValueError("MarketApi: There was a problem with the provided start_time. \n%s" % str(ve))
    try:
      datetime_end_time = self.validate_timestamp(end_time)
    except ValueError as ve:
      raise ValueError("MarketApi: There was a problem with the provided end_time. \n%s" % str(ve))

    barsets = self.get_barsets(tickers, datetime_start_time, datetime_end_time)
    if barsets is None:
      raise ValueError("Error Alpaca API was unable to retrieve barset data for the constituent stocks.")
    tickers_return_dict = {}
    for ticker in tickers:
      if ticker not in barsets or len(barsets[ticker]) < 2:
        continue
      barset = barsets[ticker]
      tick_open = barset[0].o
      tick_close = barset[-1].c
      tickers_return_dict[ticker] = (tick_close - tick_open) / tick_open * 100

    return tickers_return_dict

  def plot_index(self, index_name, constituent_tickers, start_time, end_time, save_path):
    """
    Parameters  - index_name:          The name of the index to plot
                  constituent_tickers: list of type str containing the index's constituent tickers
                  start_time:          str of start time
                  end_time:            str of end time
                  save_path:           str specifying the path to write the resulting matplotlib figure as a .png.   
    
    Note: The formats supported for the provided times are as follows:
            - ISO
            - YYYY/MM/DD
            - YYYY\MM\DD
            - YYYY-MM-DD

    Return      - A str representing the path of the created matplotlib figure PNG 

    Description - Provided a list of tickers and a timeframe, save a .PNG matplotlib figure representing a line chart of the `index`
                  which is composed of the provided constituent tickers. matplotlib cannot create a .PNG line chart as a child thread
                  so a new Process is spawned solely for this matplotlib functionality.
    """
    datetime_start_time = self.validate_timestamp(start_time)
    datetime_end_time = self.validate_timestamp(end_time)

    barsets = self.get_barsets(constituent_tickers, datetime_start_time, datetime_end_time)
    if barsets is None:
      raise ValueError("Error Alpaca API was unable to retrieve barset data for the constituent stocks.")
    (index_return_plots, plot_dates) = self.get_return_plots(constituent_tickers, barsets)

    positive = False
    if index_return_plots[len(index_return_plots) - 1] > 1:
        positive = True

    manager = Manager()
    return_dict = manager.dict()
    p = Process(target=plot_stock, args=(return_dict, plot_dates, index_return_plots, index_name, positive, save_path))
    p.start()
    p.join()

    return(return_dict.values()[0])

  def get_barsets(self, tickers, start_time, end_time):
    """
    Parameters  - tickers:    list of type str containing tickers
                  start_time: datetime.datetime object representing the start time
                  end_time:   datetime.datetime object representing the end time

    Return      - dictionary where k=ticker_name v=list of barsets (See AlpacaAPI file so and so on line so and so) 

    Description - Wrapper function for AlpacaAPI's `get_barset()` function. This wrapper function is necessary due to AlpacaAPI limiting the maximum number of barsets
                  per request to 1,000. The amount of barsets needed is unknown until the delta between the start and end time has been determined. This wrapper function
                  will invoke the AlpacaAPI `get_barset()` function as many times as needed to obtain the complete barset dictionary which fulfills the desired timespan.
                  The time delta determines the bars/time value (1Min, 5Min, or 1D), and in turn the total amount of bars per ticker needed.
                  If the amount of bars per ticker exceeds AlpacaAPI's limit of 1,000 then let the amount of requests needed per ticker be equal to
                  `math.ceil(bars_per_ticker/1000)` 
    """
    timeframe = ""
    bars_per_ticker = None
    time_delta = end_time-start_time
    if time_delta <= datetime.timedelta(days=1):
      """
      If the delta between start and end time is less than a day, get a barset for each minute of market's open (390).
      """
      bars_per_ticker = 390
      timeframe = "1Min"
    elif time_delta > datetime.timedelta(days=7):
      """
      If the delta between start and end time is greater than a week, get a barset for each open market day.
      """
      bars_per_ticker = math.ceil(time_delta.days * .71) #rough ratio of market days to total days including weekends and holidays
      print("1day")
      timeframe = "1D"
    else:
      """
      If the delta between start and end time is greater than a day but less than a week, get a barset on a 5 minute basis for each open market day.
      """
      #quote every 5 minutes equates to 78 total in a single day
      bars_per_ticker = time_delta.days * 78
      print("5Min")
      timeframe = "5Min"

    barsets = {}
    requests_to_fulfill = float(bars_per_ticker) / 1000  
    if requests_to_fulfill > 1:
      """
      Since more than one request is needed to obtain all of the bars for a given ticker we need to determine the
      individual timeframes needed for the requests. This scenario is only possible if time span exceeds 1 week
      """
      timeframe_tuples = []
      time_chunk_per_request = time_delta / requests_to_fulfill
      start_time_chunk = start_time
      for x in range( math.ceil(requests_to_fulfill) ):
        end_time_chunk = start_time_chunk + time_chunk_per_request
        timeframe_tuples.append((start_time_chunk, end_time_chunk))
        start_time_chunk = end_time_chunk + datetime.timedelta(days=1) #increment start_time by timeframe chunk
      for ticker in tickers:
        """
        Now that we have the constituent timeframes needed to compose the total timeframe for the requested barsets
        create a full barset dictionary entry in `barsets` for each ticker where k=ticker, v=list of barsets
        """
        ticker_barsets = []
        for time_tuple in timeframe_tuples:
          start_time_iso = self.datetime_to_iso(time_tuple[0])
          end_time_iso = self.datetime_to_iso(time_tuple[1])
          print("Start and End Times being used for query on %s: %s and %s" % (ticker, start_time_iso, end_time_iso))
          with self.mutex:
            """
            AlpacaAPI doesn't seem to like when multiple threads are attempting to use the same API instance
            """
            temp_set = self.alpaca_api.get_barset(ticker, timeframe=timeframe, limit=1000, start=start, end=end)
          if len(temp_set[ticker]) > 0:
            ticker_barsets.extend(temp_set[ticker])
          if len(ticker_barsets) > 0 and ticker_barsets is not None:
            barsets[ticker] = ticker_barsets
    elif requests_to_fulfill * len(tickers) > 1:
      """
      If the amount of barsets for all of the requested tickers exceeds 1,000, but the amount of barsets for each ticker's request 
      by itself is under 1,000 then for simplicity make a single AlpacaAPI request per ticker to create the full barset dictionary, `barsets`.  
      """
      start_time_iso = self.datetime_to_iso(start_time)
      end_time_iso   = self.datetime_to_iso(end_time)
      for ticker in tickers:
        with self.mutex:
          barset = self.alpaca_api.get_barset(ticker, timeframe=timeframe, limit=1000, start=start_time_iso, end=end_time_iso)
        if len(barset[ticker]) > 0 and barset[ticker] is not None: 
          barsets[ticker] = barset[ticker]
    else:
      """
      All the barsets needed for all the provided tickers can be obtained in a single AlpacaAPI request.
      """
      start_time_iso = self.datetime_to_iso(start_time)
      end_time_iso = self.datetime_to_iso(end_time)
      with self.mutex:
        barsets = self.alpaca_api.get_barset(tickers, timeframe=timeframe, limit=1000, start=start_time_iso, end=end_time_iso)

    #below code is for removing the tickers from the barsets that AlpacaApi was unable to retrieve data for. 
    final_barsets = {}
    for k, v in barsets.items():
      if len(v) > 0:
        final_barsets[k] = v
      else:
        print("AlpacaAPI was unable to retrieve barset data for ticker %s. Skipping . . ." % k)
    if len(final_barsets) > 0:
      return final_barsets
    else:
      return None

  def get_return_plots(self, tickers, barsets):
    """
    Parameters  - tickers: list of type str containing tickers
                  barsets: dictionary where k=ticker, v=list of bars

    Return      - index_day_returns: list of constituent ticker averages (y) 
                  plot_trade_dates:  list of time values corresponding to each constituent ticker average (x)

    Description - Provided a dictionary of barsets obtain the value of each return plot point needed to compose the line chart for the "index" derived from `tickers`.
                  The value of each plot point (x,y) is obtained by plotting the average of all the ticker returns for a specified time chunk (y) against the specified time (intervals by a width of 1Min, 5Min, or 1Day) (x).
                  Since it is possible for two or more stocks defined in `tickers` to have a different amount of bars, special precaution is needed to verify each plot point
                  representing the index's overall return line chart is derived from only those tickers who had stock activity within the given time interval. 
                  This scenario occurs when a constituent stock IPOs after the specified start date for the line chart. 
    """
    plot_trade_dates = []
    largest_set = -1
    largest_set_ticker = None
    for ticker in tickers:
      """
      Determine the ticker which has the largest set of bars, and in turn the greatest timeframe.
      """
      if ticker in barsets and len(barsets[ticker]) > largest_set:
        largest_set = len(barsets[ticker])
        largest_set_ticker = ticker

    for k in range(len(barsets[largest_set_ticker])):
      """
      Obtain every x=(time as a Unix epoch in seconds) value needed by iterating through largest spanning barset
      """
      plot_trade_dates.append(barsets[largest_set_ticker][k].t)

    starting_prices = {}      #k=ticker, v= float: opening price of ticker's stock on earliest bar
    tickers_return_plots = [] #list containing lists of calculated returns for each constituent ticker 
    for ticker in tickers:
      """
      For each ticker obtain its return at each time interval (y) with respect to its initial opening value from its earliest bar.
      """
      if ticker not in barsets:
        continue
      barset = barsets[ticker]
      return_plots = []
      return_plots.append(1) #initialize base
      earliest_open = barset[0].o
      for x in range(1, len(barset)):
        """
        For each bar derive a return from its closing value and the earliest open.
        """
        #print("Price: %3f And Time:%s" % (barset[x].c, barset[x].t))
        return_plots.append((barset[x].c - earliest_open) / earliest_open * 100)
      tickers_return_plots.append(return_plots)

    intervals_processed = 0
    index_day_returns = []
    while (largest_set) > 0:
      """
      Since it is presumed that not every constituent ticker will have the same start date defining its barset interval (such as in the case of a recently IPOd constituent),
      we will iterate the returns backwards when deriving each net return plot point (y). This is because we can safely assume each constituent ticker all have the same end date,
      and upon running over the last (or rather first) return for a recently IPOd ticker it can be easily excluded when calculating subsequent return averages by tracking the amount of intervals proccessed and the size of the barsets.
      """
      total_day_return = 0
      total_barsets = 0
      for return_plot in tickers_return_plots:
        """
        Where len(tickers_return_plots) == len(tickers) obtain the return from each constituent ticker for the current timeframe
        and add it to total_day_return. If a return value is not available for a given constituent ticker under the current timeframe,
        then omit it from the calculation and do not increment `total_barsets`, which is used the denominator for calculating the average return.
        """
        if len(return_plot) > intervals_processed:
          total_day_return = total_day_return + return_plot[(len(return_plot) - 1) - intervals_processed] 
          total_barsets = total_barsets + 1
      index_day_returns.insert(0, (total_day_return / total_barsets)) #Insert at beginning, which will result in the most recent timestamp occuring at the end
      intervals_processed = intervals_processed + 1
      largest_set = largest_set - 1

    return (index_day_returns, plot_trade_dates)

  def validate_ticker(self, ticker):
    """
    Parameters  - ticker: str containing ticker to validate

    Return      - ticker: str containing validated and sanitized ticker

    Description - Determine if provided ticker is of valid format and exists on the markets
    """
    if ticker.startswith("$"):
      ticker = ticker[1:]
    
    if len(ticker) > 5:
      return None
    for char in ticker:
      if char.isalpha() is False:
        return None
    ticker = ticker.upper()
    try:
      with self.mutex:
        last_quote = self.alpaca_api.get_last_quote(ticker)
    except Exception as e:
      print("404 ticker not found %s" % ticker)
      return None
    else:
      print(last_quote)
      return ticker

  def validate_timestamp(self, timestamp):
    """
    Parameters  - timestamp: either datetime.datetime object or str representing datetime

    Return      - datetime.datetime object

    Description - Determine if parameter `timestamp` is of type datetime. If so return, else
                  check if str representing datetime, if so convert using dateutil.parser, else
                  raise Exception.
    """
    if isinstance(timestamp, datetime.datetime):
      print("timestamp.timestamp obj")
      return timestamp
    elif isinstance(timestamp, str):
      if len(timestamp.split("/")) >= 3:
        timestamp = timestamp.replace("/", "-")
      elif len(timestamp.split("\\")) >= 3:
        timestamp = timestamp.replace("\\", "-")
      try:
        print("In validate timestamp. The str time: %s before dateutil.parse" % timestamp)
        datetime_timestamp = dateutil.parser.isoparse(timestamp)
      except ValueError as ve:
        raise ValueError("Time provided is not a valid time in ISO format.")
      except OverflowError as oe:
        raise ValueError("Time provided is in the future.")
      else:
        return datetime_timestamp
    else:
      raise ValueError("Time provided is an unrecognized type for time in ISO format.")

  def datetime_to_iso(self, datetime_obj):
    """
    Parameters  - datetime_obj: datetime.datetime object

    Return      - str representation of provided datetime.datetime obj in ISO format

    Description - Converts a datetime.datetime object into a string in ISO format.
    """
    time_str = str(datetime_obj)
    if len(time_str.split(" 00:00:00")) == 2:
      time_str = time_str.replace(" 00:00:00", "T09:30:00-04:00")
    elif len(time_str.split(" ")) == 2:
      time_str = time_str.replace(" ","T")
    return time_str


  def get_index_tickers(self, discord_user_id, index_name):
    """
    Parameters  - discord_user_id: str representing the Discord user ID
                  index_name:      str representing the name of the index/portfolio to obtain the constituent tickers from

    Return      - list of string objects containing tickers

    Description - MarketApi wrapper function which returns a list of strings containing tickers.
    """
    try:
      (db_user_id, exists) = self.market_db.get_user_id(discord_user_id)
    except Exception as e:
      raise ValueError(str(e))
      return
    else:
      try:
        return self.market_db.get_portfolio_tickers(db_user_id, index_name)
      except Exception as e:
        raise ValueError(str(e))

  def get_user_indexes(self, discord_user_id):
    """
    Parameters  - discord_user_id: str representing the Discord user ID

    Return      - list of string objects containing names of the indexes belonging to the Discord user

    Description - MarketApi wrapper function which returns a list of strings containing index names.
    """
    try:
      (db_user_id, exists) = self.market_db.get_user_id(discord_user_id)
    except Exception as e:
      raise ValueError(str(e))
      return
    else:
      try:
        return self.market_db.get_index_names(db_user_id)
      except Exception as e:
        raise ValueError(str(e))


  def set_env_vars(self):
    """
    Parameters  - void

    Return      - void

    Description - init function for reading the .ini containing AlpacaAPI connection information.
    """
    config = configparser.ConfigParser()
    config.read("./discord_bot_runner/keys/alpaca_api_keys.ini")
    os.environ["APCA_API_BASE_URL"] = config['api_keys']['base_url'] 
    os.environ["APCA_API_KEY_ID"] = config['api_keys']['api_key']
    os.environ["APCA_API_SECRET_KEY"] = config['api_keys']['secret_key']



  class MarketDatabaseHelper():
    """
    Helper inner-class to provide an interface to the MySQL database
    """
    def __init__(self):
      super().__init__()
      config = configparser.ConfigParser()
      config.read("./discord_bot_runner/keys/db_keys.ini")
      self.mysql_cnx = mysql.connector.connect(
                          host = config['db_keys']['host'],
                          user = config['db_keys']['user'],
                          passwd = config['db_keys']['password'],
                          db = config['db_keys']['db']
                        )

    def get_user_id(self, discord_user_id):
      """
      Parameters  - discord_user_id: str representing Discord user ID with no `<@!` preprended or `>` appended

      Return      - int:     database user_id
                    boolean: user_exists

      Description - Gets the equivalent database ID from provided Discord User ID. If entry in database does not exist then make one. 
      """
      user_id = -1
      user_exists = False
      cursor = self.mysql_cnx.cursor()
      
      try:
        cursor.execute(MYSQL_SELECT_USER, (discord_user_id,))
      except mysql.connector.IntegrityError as er:
        print(er)
        cursor.close()
        return (user_id, user_exists)

      #rows are tuples
      row = cursor.fetchone()
      if row is None:
        #user doesn't exist so create
        try:
          cursor.execute(MYSQL_ADD_USER, (discord_user_id,))
        except:
          cursor.close()
          return (user_id, user_exists)
        else:
          self.mysql_cnx.commit()
          user_id = cursor.lastrowid
      else:
        user_exists = True
        user_id = row[0]

      cursor.close()
      return (user_id, user_exists)

    def insert_portfolio(self, name, tickers, format, user_id):
      """
      Parameters  - name:    str containing name of portfolio/index to be created.
                    tickers: list of strings representing stock tickers 
                    format:  int representing format code for portfolio/index to be created
                    user_id: int database ID (can be derived from Discord user ID) 

      Return      - str representation of provided datetime.datetime obj in ISO format

      Description - Provided a list of tickers create a new index/portfolio called `name` if one does not already exist. If index name/user_id
                    combination already exists then return 0. Else if no error then return -1.
      """
      cursor = self.mysql_cnx.cursor()
      portfolio_id = None

      try:
        print(MYSQL_SELECT_PORTFOLIO % (name, user_id))
        cursor.execute(MYSQL_SELECT_PORTFOLIO, (name, str(user_id),))
      except mysql.connector.IntegrityError as er:
        print(er)
        cursor.close()
        return -1

      #rows are tuples
      row = cursor.fetchone()
      cursor.close()
      if row is None:
        cursor = self.mysql_cnx.cursor()
        try:
          cursor.execute(MYSQL_CREATE_PORTFOLIO, (name, str(user_id),))
        except Exception as e:
          print("we here12243346")
          print(str(e))
          cursor.close()
          return -1
        else:
          portfolio_id = cursor.lastrowid
          self.mysql_cnx.commit()
          cursor.close()
      else:
        return 0


      for ticker in tickers:
        ticker_name = ""
        if isinstance(ticker, tuple):
          ticker_name = ticker[0]
        else:
          ticker_name = ticker

        print("ticker:%s  return val:%d" % (ticker_name, self.add_stock(ticker_name, portfolio_id)))

      return portfolio_id
    
    def get_index_names(self, user_id):
      """
      Parameters  - user_id: int containing the database user ID to query for index names on

      Return      - list of strings containing the names of indexes/portfolios that exist under `user_id`

      Description - Provided a a database user ID, retrieve the names of all the existing indexes/portfolios which the provided user owns.
      """
      cursor = self.mysql_cnx.cursor()
      try:
        cursor.execute(MYSQL_SELECT_INDEX_NAMES, (user_id,))
      except mysql.connector.IntegrityError as er:
        print(er)
        cursor.close()
        raise ValueError("Database error. Contact admin.")

      row = cursor.fetchone()
      index_names = []
      while row:
        index_names.append(row[0])
        row = cursor.fetchone()
      cursor.close()
      
      return index_names



    def get_portfolio_tickers(self, user_id, name):
      """
      Parameters  - user_id: int containing the database user ID to be used in query
                    name:    str containing the name of the index/portfolio to obtain the tickers of

      Return      - list of strings containing the names of the tickers composing the portfolio/index.

      Description - Provided a database user ID and the name of a index/portfolio, obtain a list of all the constituent tickers
      """
      cursor = self.mysql_cnx.cursor()

      try:
        print(MYSQL_SELECT_PORTFOLIO % (name, user_id))
        cursor.execute(MYSQL_SELECT_PORTFOLIO, (name, str(user_id),))
      except mysql.connector.IntegrityError as er:
        print(er)
        cursor.close()
        raise ValueError("Database error. Contact admin.")

      row = cursor.fetchone()
      cursor.close()

      if row is None:
        raise ValueError("Index, %s, does not exist for the following user: %s" % (name, user_id))
        
      portfolio_id = row[0]
      cursor = self.mysql_cnx.cursor()
      try:
        cursor.execute("SELECT ticker FROM stocks WHERE index_id = %d" % portfolio_id)
      except mysql.connector.IntegrityError as er:
        print(er)
        cursor.close()
        raise ValueError("Database error. Contact admin.")
      
      tickers = []
      row = cursor.fetchone()
      while row:
        tickers.append(row[0])
        row = cursor.fetchone()
      cursor.close()

      if len(tickers) == 0:
        #raise ValueError("No tickers found for index: %s" % name)
        print("No tickers found for index: %s" % name)
        return None
      else:
        return tickers

    def add_stock(self, ticker, portfolio_id):
      """
      Parameters  - ticker:       str containing ticker to add to the portfolio/index
                    portfolio_id: str containing the name of the portfolio/index to add a stock to

      Return      - int: 0 for success and -1 for a database error.

      Description - Provided a ticker and the ID of an index/portfolio, add the ticker to the portfolio 
      """
      cursor = self.mysql_cnx.cursor()

      #need to validate stock here
      try:
        cursor.execute(MYSQL_SELECT_STOCK, (ticker, str(portfolio_id),))
      except mysql.connector.IntegrityError as er:
        print(er)
        print("error on query: %s" % MYSQL_SELECT_STOCK % (ticker, str(portfolio_id)))
        cursor.close()
        return -1

      row = cursor.fetchone()
      cursor.close()
      if row is None:
        try:
          cursor = self.mysql_cnx.cursor()
          cursor.execute(MYSQL_ADD_STOCK, (ticker, str(portfolio_id),))
        except Exception as e:
          print(str(e))
          print("Hello we are right here")
          cursor.close()
          return -1
        else:
          self.mysql_cnx.commit()
          stock_id = cursor.lastrowid
          cursor.close()
          return stock_id
      else:
        return 0
    
def plot_stock(return_dict, x, y, plot_name, positive, save_path):
  """
  Parameters  - x:         list containing values for defining the x-axis of the matplotlib graph  
                y:         list containing values for defining the y-axis of the matplotlib graph
                plot_name: str containing the title and name of the matplotlib graph
                positive:  boolean to indicate if chart finishes positive or negative (used to determine green or red)
                save_path: str containing the path to save the matplotlib graph as a PNG to.

  Return      - void

  Description - Provided a list of equal lengths containing x and y values, plot a matplotlib graph and save as a PNG to specified path.
  """
  file_path = None
  row_count = len(x)
  col_count = len(y)
  if row_count != col_count:
    matplotlib_queue.put(None)
    return
  #fig, ax = plt.subplots( nrows=row_count, ncols=col_count )
  #print("i think we just made a skeleton plot. now we're about to plot for real")
  color = None
  fig = plt.figure()
  if positive:
    plt.plot(x, y, "g-")
  else:
    plt.plot(x, y, "r-")
  fig.suptitle("%s" % plot_name, fontsize=20, color="green")
  plt.xticks(rotation=50)
  plt.tight_layout()
  file_path = save_path.replace("\\", "\\\\") + "\\\\%s__%s.png" % (plot_name.replace(" ", "_"), str(datetime.datetime.now()).replace(" ", "_").replace(":","_").replace(".","_")) 
  plt.savefig(file_path)
  return_dict[plot_name] = file_path 
  return

def path_leaf(path):
  head, tail = ntpath.split(path)
  return tail or ntpath.basename(head)