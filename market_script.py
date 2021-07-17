from abc import ABC, abstractmethod

import threading
import queue

class MarketScript(ABC, threading.Thread):
  isBase = True
  def __init__(self, *args, **kwargs):
    threading.Thread.__init__(self)
    if 'thread_ID' in kwargs is False or 'thread_name' in kwargs is False or 'parent_queue' in kwargs is False or 'market_api' in kwargs is False:
      raise KeyError("Error: \"thread_ID\", \"thread_name\", \"event_queue\", and \"parent_queue\" must be provided to the BehaviorSet's ctor {k=v}")
    else:
      self.threadID = kwargs['thread_ID']
      self.name = kwargs['thread_name']
      self.parent_queue = kwargs['parent_queue']
      self.market_api = kwargs['market_api']

  def run(self):
    self.market_script_logic()

  @abstractmethod
  def market_script_logic(self):
    pass
