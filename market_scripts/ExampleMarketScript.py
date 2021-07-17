import sys
import time
from ..market_script import MarketScript
import os
import json
from multiprocessing import Process, Manager

class MySecondMarketScript(MarketScript):
  isBase = False
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def market_script_logic(self):
    tickers = ['SE', 'FUBO', 'MSFT']
    plot_path = self.market_api.plot_index("Lets go", tickers, '2020-01-01T09:30:00-04:00', '2021-03-12T09:30:00-04:00', "./")
    json_obj = '{"thread_id":%d, "data_type":"upload", "file_path":"%s"}' % (self.threadID, plot_path)
    self.parent_queue.put(json_obj)