import discord
import requests
import ntpath
import inspect
from .behavior_set import BehaviorSet
from .market_api import MarketApi
from .market_script import MarketScript
import queue
import json
import os
from importlib import import_module, reload
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
import threading

help_new_index    = "- `new_index(<index_name>)`\\n"
help_plot_index   = "- `plot_index(@discord_user, <index_name>, start_time, end_time)`\\n" 
help_get_best     = "- `get_best_stocks(@discord_user, (@discord_user, <index_name>), (@discord_user, [array_of_index_names]),`\\n                                     `start_time, end_time, limit=int (optional))`\\n"
help_get_worst    = "- `get_worst_stocks(@discord_user, (@discord_user, <index_name>), (@discord_user, [array_of_index_names]),`\\n                                      `start_time, end_time, limit=int (optional))`\\n"
help_show_scripts = "- `show_scripts()`\\n"
help_run_script   = "- `<custom_script_name>()`  --> Runs uploaded py/se script as if it were a market cmd.\\n"
help_cron_job     = "- `add_cron_job(script=<script_name>, year='XXXX', month='1-12', day_of_week='0-6', hour='0-23', minute='0-59')`\\n"
help_se_script    = "- `new_se_script(<script_name>)`\\n"
help_file_drop    = "- To add custom script or csv index, simply drag and drop the .py or .csv files, respectively, combined with the usual `./market` cmd.\\n"
help_func_info    = "To get more information on a market command, provide the name of the command as an argument to the `help()` function."

help_function_descriptions = ["`plot_index()`: Provided an index name and its discord owner, plot a return percentage within the specified timeframe using matplotlib.",
                              "`get_best_stocks()`: Provided one or more discord users, get the best stocks among all indexes across the provided discord users. Or provided a pair containing a discord user and an index name, get best stocks from the specific index. Or provided a pair containing a discord user and an array of index names belonging to the user, get best stocks among the indexes. Any of these arg types can be provided as a mixture.",
                              "`get_worst_stocks()`: Same as get_best_stocks() but worst stocks.",
                              "`new_index()`: Provided an index name, the Market Bot will prompt you for valid tickers. Hit `:w` when complete.",
                              "`show_scripts()`: Shows the available scripts to run which have been uploaded by discord users.",
                              "`<custom_script_name>()`: Runs the custom uploaded python script as if it were a market command.",
                              "`add_cron_job()`: Schedules a given python or se script as a unix-like cron job.",
                              "`new_se_script()`: Market Bot will prompt the Discord user for valid `./market` commands until the user hits :w. Then saves the .se file under the name provided as an argument.",
                              "`help()`: Displays the function signatures of the available Market functions for usage. When provided the name of a function as an argument display the function's description."
                              ]


class MarketBehaviorSet(BehaviorSet):
  """
  A BehaviorSet (Discord Bot) for providing stock data and API functionality to a Discord Guild and its users.
  """
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.market_api = None
    self.thread_count = 0

    try:
      self.market_api = MarketApi()
    except Exception as e:
      print(e)
      raise ValueError("Failed to initialize the MarketApi!")

    self.script_results_queue = queue.Queue() #message queue for child threads (Market commands) to push results to
    self.threadID_to_channelID = {} #k=thread_ID,v=channel_ID - Used to track what channel a market command derived from
    self.script_name_to_mod = {}    #map of module name to its imported module object

    self.uploaded_mods = {}    #map of uploaded module names (can be .se module or .py module) to their class object (if .py) or to the module path (if .se)
    self.scheduler = BackgroundScheduler()

    self.open_index_files = {}  #k=user_id, v=(index_name, []) where list is list of tickers
    self.open_se_scripts = {}

    self.mutex = threading.Lock() #to protect self.thread_count as multiple threads will be modifying this member asynchronously

  def handle_discord_event_loop(self):
    """
    Parameters  -
    Description - Infinite loop to continuously monitor its event queue and its queue for child threads. 
                  Event queue messages (self.event_queue) derive from user input from a Discord channel and are handled and passed down from the DiscordBotRunner thread. 
                  Messages from child threads (self.script_results_queue) indicate a completed market command and contain output data to be pushed to the DiscordBotRunner message queue. 
                  If there is an event in the self.event_queue then determine if it's prepended with './market'. If not prepended with './market' then
                  check if the user sending the message currently has an open file (new stock index creation or se script). If so parse input and write to file, else ignore message.
                  If the message is a valid './market' command and there are no attachments then spawn 'parse_and_run_market_cmd' as a new thread to run the command. 
                  The thread spawned for the './market' command will populate the scripts result queue upon completion. If the './market' command has one or more attachments then determine
                  if the user is attempting to upload a .se, .csv, or .py file (See README for file uploads).
    """
    self.scheduler.start()
    while True:
      try:
        message = self.event_queue.get_nowait()
      except:
        try:
          #if no event from discord channels then check to see if any currently running scripts have pushed items to the queue, if so forward to parent, else continue loop
          result_item = self.script_results_queue.get_nowait()
        except:
          continue
        else:
          """
          Fix format on written scripts and document the format of JSON messages in README
          """
          #market commands are spawned as a new thread because multiple users can send commands simultaneously and we don't want the main MarketBehaviorSet thread getting blocked
          json_data = json.loads(result_item)
          channel_id = self.threadID_to_channelID[json_data['thread_id']]
          data_type = json_data['data_type']

          if data_type == "text_message":
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", json_data['data'])
          elif data_type == "upload":
            json_obj = '{"channel":%d, "data_type":"%s", "file_path":"%s"}' % (channel_id, "upload", json_data['file_path'])
          elif data_type == "text/upload":
            json_obj = '{"channel":%d, "data_type":"%s", "file_path":"%s", "data":"%s"}' % (channel_id, "text/upload", json_data['file_path'], json_data['data'])
          else:
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Invalid JSON format for provided script. See README.")

          self.parent_queue.put(json_obj)
      else:
        user_id = message.author.id
        channel_id = message.channel.id

        if message.attachments and message.content.startswith("./market"):
          self.handle_message_attachments(message.attachments, user_id, channel_id)
        elif user_id in self.open_index_files:
          """
          Evaluates to True if user providing data previously initiated an incomplete 'new_index()' ./market command.
          The message is passed into 'self.handle_file_write' and is expected to contain a single stock ticker or EOF. 
          """
          json_data = self.handle_file_write(user_id, channel_id, message.content)
          if json_data:
            self.parent_queue.put(json_data)
          continue
        elif user_id in self.open_se_scripts:
          """
          Evaluates to True if user providing data previously initiated an incomplete 'new_se_script()' ./market command.
          The message is passed into 'self.handle_se_script_write' and is expected to contain a single ./market command or EOF. 
          """
          json_data = self.handle_se_script_write(user_id, channel_id, message.content)
          if json_data:
            self.parent_queue.put(json_data)
          continue
        elif message.content.startswith("./market "):
          """
          Evaluates to True if user provides './market' command without an attachment. 
          Will spawn the ./market command as a new thread because multiple users can send ./market commands simultaneously
          and we do not want the main MarketBehaviorSet thread to get blocked.
          """
          try:
            threading.Thread(target=self.parse_and_run_market_cmd(message.content, user_id, channel_id)).start()
          except Exception as e:
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
          continue
       #else ignore
              
  def handle_message_attachments(self, attachments, user_id, channel_id):
    """
    Parameters  - attachments: list containing the Discord Message file attachments
                  user_id:     str of Discord user ID
                  channel_id:  str of Discord channel ID
    Description - This function handles attachments uploaded by users through registered Discord channels. It supports handling the uploading of .csv files (of tickers),
                  ".se files" (custom market command scripts), and .py files (MarketScript implementations). 
    """
    py_files = []
    for attachment in attachments:
      if attachment.url.endswith('.csv'):
        """
        If user uploads a .csv file assume it is a comma separated list of stock tickers. Get the request and call 'market_api.create_portfolio', passing the ticker data along.
        'market_api.create_portfolio' will insert a new portfolio/index for the user if the .csv is of valid format and contains valid tickers and return a status message.
        """
        try:
          #may want the below function to return a status message
          try:
            #fudge url here to verify behavior
            r = requests.get(url = attachment.url)
          except requests.exceptions.RequestException as e:
            raise ValueError("Bad URL for csv file uploaded. Contact admin.")
          portfolio_name = path_leaf(attachment.url)[:-4]
          rval = self.market_api.create_portfolio(user_id, r.text, portfolio_name)
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", rval)
          self.parent_queue.put(json_obj)
        except ValueError as ve:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(ve))
          print(json_obj)
          self.parent_queue.put(json_obj)
      elif attachment.url.endswith('.py'):
        """
        If user uploads .py files assume user is attempting to upload a MarketScript implementation. Append url to local list in case user uploads dependencies
        """
        py_files.append(attachment.url)
      elif attachment.url.endswith('.se'):
        """
        If attachment is a .se file upload and set self.uploaded_mods
        """
        try:
          rval = self.handle_se_script_upload(attachment.url)
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", rval)
          self.parent_queue.put(json_obj)
        except Exception as e:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
          self.parent_queue.put(json_obj)
    if len(py_files) > 0:
      """
      Pass list of .py urls to 'self.handle_script_upload' to import each module and to obtain the class object of the MarketScript.
      (Note multiple .py files can be uploaded and imported as external dependencies for the MarketScript)
      """
      try:
        (name, market_script_obj) = self.handle_script_upload(py_files)
      except Exception as e:
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
        self.parent_queue.put(json_obj)
      else:
        overwrite = False
        if name in self.uploaded_mods:
          overwrite = True
        #if instantiation of uploaded MarketScript implementation already exists then overwrite it.
        #Note the MarketScript has merely been instantiated and its thread has not been started.
        self.uploaded_mods[name] = market_script_obj
        if overwrite:
          success_msg = "Successfully overwrote Python Module: `%s`. It is now available for use." % name
        else:
          success_msg = "Successfully uploaded Python Module: `%s`. It is now available for use." % name
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", success_msg)
        self.parent_queue.put(json_obj)

  """
  Parsing and validation functions
  """
  def parse_and_run_market_cmd(self, market_cmd, user_id, channel_id):
    """
    Parameters  - market_cmd: str containing './market <args>' to parse and run
                  user_id:    str of Discord user ID
                  channel_id: str of Discord channel ID
    
    Return      - void

    Description - This function will parse, validate and run a single ./market command provided via 'market_cmd'. Initially determines if the ./market command exists 
                  and is of valid syntax. If valid ./market command then obtain the market command ID (1-9) to determine course of action. The result of the command is pushed
                  to the parent queue (self.parent_queue) as a JSON object.
    """
    try:
      """
      specific parsing function for each market cmd. each one will raise an ValueError exception if input is incorrect
      else functions will return a tuple of valid arguments to be passed in for its corresponding simple market api call
      """
      (function_id, func_args) = self.validate_market_command(market_cmd)

      #plot_index()
      if function_id == 1: 
        try:
          (user_id, index_name, start_time, end_time) = self.parse_plot_index_arguments(func_args)
        except Exception as e:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
        else:
          try:
            print("index name: %s...." % index_name)
            tickers = self.market_api.get_index_tickers(user_id, index_name)
          except ValueError as ve:
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(ve))
          else:
            if tickers:
              data_upload_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "market_scripts")
              data_upload_path = os.path.join(data_upload_path, "data")
              if not os.path.exists(data_upload_path):
                os.makedirs(data_upload_path)
              try:
                plot_path = self.market_api.plot_index(index_name, 
                                                      tickers,
                                                      start_time, end_time, data_upload_path)
              except Exception as e:
                json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
              else:
                json_obj = '{"channel":%d, "data_type":"%s", "file_path":"%s"}' % (channel_id, "upload", plot_path)
            else:
              json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "The specified index does not contain any tickers.")

      #get_best/worst_stocks()
      elif function_id == 2 or function_id == 3:
        (query_limit, start_time, end_time, func_args) = self.ordered_stocks_parse_times_and_limit(func_args)
        user_index_dict = self.ordered_stocks_parse_users_and_indexes(func_args) #parses the specified users and indexes from func_args and returns a dict where k=user v=([index_names])
        all_stocks = self.ordered_stocks_get_from_user_index_dict(user_index_dict) #provided dictionary derived from parsing users and specified indexes, get all stocks and their returns from all relevant indexes, sort them
        try:
          print("madmamame it here")
          ticker_returns_dict = self.market_api.get_return(all_stocks, start_time, end_time)
        except Exception as e:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))
        else:
          sorted_returns = dict(sorted(ticker_returns_dict.items(), key=lambda item: item[1]))
          sorted_returns_msg = self.ordered_stocks_get_message(sorted_returns, function_id, query_limit)

          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", sorted_returns_msg)

      #new_index()
      elif function_id == 4: 
        index_name = func_args.replace('“','"').replace('”','"')
        index_name = index_name.strip('\"')
        if index_name is None or index_name == "":
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: Argument containing Index Name Expected.")
        else:
          self.open_index_files[user_id] = (index_name, [])
          start_new_index_prompt = "New Index `%s` Opened for <@%s>. Provide stocks by enterting in one symbol per one Discord message at a time. Send `:w` when complete." % (index_name, user_id)
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", start_new_index_prompt)

      #show_scripts()
      elif function_id == 5: 
        if len(func_args) == 0:
          available_scripts = []
          for k, v in self.uploaded_mods.items():
            available_scripts.append("`" + k + "`")
          if len(available_scripts) == 0:
            available_scripts.append("No scripts yet uploaded.")
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Scripts: %s" % (', ').join(available_scripts))
        else:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: show_scripts() takes no arguments.")

      #Run custom script (.py or .se)
      elif function_id == 6:
        script_name = func_args
        print(func_args)
        if isinstance(self.uploaded_mods[script_name], str):
          self.run_se_script(self.uploaded_mods[script_name], user_id, channel_id)
        else:
          market_script_obj = self.uploaded_mods[script_name]
          #mutex
          with self.mutex:
            """
            Since parse_and_run_market_cmd can be spawned as a separate thread with multiple concurrently running, the thread_count needs to be protected
            """
            market_obj = market_script_obj(thread_ID=self.thread_count, thread_name="%s" % script_name + str(self.thread_count), parent_queue=self.script_results_queue, market_api=self.market_api)
            self.threadID_to_channelID[self.thread_count] = channel_id
            self.thread_count = self.thread_count + 1
          market_obj.start()
          return

      #add_cron_job()
      elif function_id == 7: 
        key_args = func_args.split(',')

        cron_args = {
          'script':None,
          'year':str(datetime.datetime.now().year),
          'month':None,
          'day_of_week':None,
          'hour':None,
          'minute':None
        }
        valid_args = True
        parse_error_msg = ""
        for karg in key_args:
          try:
            (name, value) = self.parse_cron_job_arg(karg)
          except ValueError as ve:
            parse_error_msg = parse_error_msg + " " + str(ve)
            continue
          else:
            if cron_args[name] is None or name == 'year': #replace default year
              cron_args[name] = value
            else:
              parse_error_msg = parse_error_msg + " Duplicate %s" % name 
              valid_args = False
              break
        
        for k, v in cron_args.items():
          if v is None:
            parse_error_msg = parse_error_msg + " Missing arg: %s" % k
            valid_args = False
            break

        if valid_args:
          try:
            trigger = CronTrigger(year=cron_args['year'], month=cron_args['month'], day_of_week=cron_args['day_of_week'], hour=cron_args['hour'], minute=cron_args['minute'])
          except Exception as e:
            print(str(e))
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: Invalid usage of `add_cron_job()`")
          else:
            self.scheduler.add_job(self.run_script, trigger=trigger, kwargs={'name':cron_args['script'], 'user_id':user_id, 'channel_id':channel_id})
            msg = "MarketBehaviorSet: Script, `%s`, has been successfully scheduled." % cron_args['script']
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", msg)
        else:
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "%s\\n>>>`add_cron_job(script=, year=, month=, day_of_week=, hour=, minute=)`." % parse_error_msg)

      #new_se_script()
      elif function_id == 8:
        se_script_name = func_args.replace('“','').replace('”','').replace('\"','')
        if se_script_name is None or se_script_name == "":
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: Argument containing Index Name Expected.")
        else:
          self.open_se_scripts[user_id] = (se_script_name, [])
          start_new_index_prompt = "New .se script, `%s`, opened for <@%s>. Enter market commands. Send `:w` when complete." % (se_script_name, user_id)
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", start_new_index_prompt)

      #help()
      elif function_id == 9:
        if len(func_args) > 1:
          function_id = self.validate_function(func_args)
          if function_id != -1:
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", help_function_descriptions[function_id - 1])
          else:
            json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: `%s` is not a valid market function." % func_args)
        else:
          help_message = help_new_index + help_plot_index + help_get_best + help_get_worst + help_show_scripts + help_run_script + help_cron_job + help_se_script + help_file_drop + help_func_info
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", help_message)
      else:
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "MarketBehaviorSet: Invalid Command")
    except Exception as e:
      print(str(e))
      json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", str(e))

    self.parent_queue.put(json_obj)
    return

  def validate_market_command(self, market_cmd):
    """
    Parameters  - market_cmd: str containing the ./market_cmd to validate and identify
    
    Return      - int Function ID and 
                - str func_args
                - else raises ValueException if invalid ./market command.
    
    Description - Verifies provided ./market command exists and follows correct syntax. Does not validate arguments.
    """
    function_id = -1
    valid_function = False
    #split ./market token and function call
    cmd_tokens = market_cmd.split(" ", 1)
    if len(cmd_tokens) > 1:
      function_call = cmd_tokens[1]
      function_tokens = function_call.split("(", 1)
      if len(function_tokens) > 1:
        func_args = function_tokens[1]
        close_parenth_positions = find(func_args, ')')
        if len(close_parenth_positions) < 1:
          raise ValueError("Error: No closing paranthese found on function call `%s`." % function_call)
        else:
          substr_offset = len(func_args) - close_parenth_positions[-1] 
          func_args = func_args[:-substr_offset]

        function_id = self.validate_function(function_tokens[0])
        if function_id == -1:
          raise ValueError("Error: provided function `%s`, is nonexistent." % function_tokens[0])
        elif function_id == 6:
          """
          if function_id == 6 then `function_call` contains the name of the specified custom script to be ran (assumed to be previously uploaded).
          So we are not interested in the function arguments, instead pass back the name of the function specified.
          """
          return (function_id, function_tokens[0])
        else:
          return (function_id, func_args)
      else:
        raise ValueError("Error: No closing paranthese found on function call `%s`." % function_call)
    else:
      raise ValueError("Error: `./market` must be provided a command or an attachment (See README).")
    
  def validate_function(self, name):
    """
    Parameters  - name: str containing specified ./market command to validate the existence of.

    Return      - int representing the numerical code which corresponds to the specified function, if valid. Else return -1 for invalid command.

    Description - Provided str, name, determine if name corresponds to a valid ./market command, and if so return the ./market command's 
                  corresponding numerical code. Else return -1 for invalid function.
    """
    if name == "plot_index":
      return 1
    elif name == "get_best_stocks":
      return 2
    elif name == "get_worst_stocks":
      return 3
    elif name == "new_index":
      return 4
    elif name == "show_scripts":
      return 5
    elif name in self.uploaded_mods:
      return 6
    elif name == "add_cron_job":
      return 7
    elif name == "new_se_script":
      return 8
    elif name == "help":
      return 9
    else:
      return -1

  def parse_plot_index_arguments(self, arguments_string):
    """
    Parameters  - arguments_string: str containing the arguments to be parsed

    Return      - user_id:    str containing parsed Discord user ID
                  index_name: str containing parsed index name
                  start_time: str containing parsed start date
                  end_time:   str containing parsed end date

    Description - Provided str containing every argument for the `plot_index()` command tokenize by comma.
    """
    user_id = None
    index_name = None
    start_time = None
    end_time = None
            
    token_args = arguments_string.split(",")
    arg_count = len(token_args)
    if arg_count != 3 and arg_count != 4:
      raise ValueError("Invalid number of arguments provided to `plot_index(discord_user, index_name, start_time, end_time)`")
    
    index = 0
    for arg in token_args:
      arg = arg.strip()
      if index == 0:
        if len(arg.split(" ")) > 1:
          raise ValueError("Invalid Discord user provided: %s" % arg)
        user_id = arg[2:-1]
        if user_id.startswith('!'):
          user_id = user_id[1:]
      elif index == 1:
        index_name = arg.replace('“','"').replace('”','"')
        index_name = index_name.strip("\"")
      elif index == 2:
        start_time = arg.replace('“','"').replace('”','"')
        start_time = start_time.strip("\"")
      elif index == 3:
        end_time = arg.replace('“','"').replace('”','"')
        end_time = end_time.strip("\"")
      index = index + 1

    if index == 3: 
      end_time = start_time

    print("user_id: %s\nindex_name: %s\nstart_time: %s\nend_time: %s" % (user_id, index_name, start_time, end_time))
    return(user_id, index_name, start_time, end_time)

  def ordered_stocks_parse_times_and_limit(self, arguments_string):
    """
    Parameters  - arguments_string: str containing the arguments to be parsed

    Return      - query_limit:      str containing parsed query limit if provided, else default limit (5)
                  start_time:       str containing parsed start date
                  end_time:         str containing parsed end date
                  arguments_string: str containing provided arguments_string with the start and end time strings removed

    Description - Provided string containing every argument for best/worst stocks ./market command tokenize the start and end times, and query limit.
                  Remove the start and end times from the original string to return only the contents of the string we still need to parse.
    """
    start_time = None
    end_time = None
    comma_tokens = arguments_string.split(',')
    if len(comma_tokens) > len(set(comma_tokens)): #set() will return a list without duplicate entries
      raise ValueError("Duplicate arguments were provided. That is not allowed!")
    if len(comma_tokens) > 2:
      last_token = comma_tokens[-1].strip()
      if last_token.startswith("limit="):
        arguments_string = arguments_string.replace(last_token, '')
        last_token = last_token.replace("limit=", '')
        try:
          query_limit = int(last_token)
        except:
          raise ValueError("Error parsing r-val for `limit=`")
        end_time   = comma_tokens[-2].strip()
        start_time = comma_tokens[-3].strip()
      else:
        end_time = last_token
        start_time = comma_tokens[-2].strip()
        query_limit = 5
    else:
      raise ValueError("Invalid usage of `get_best/worst_stocks function. Must provide at least one user and a start/end date")

    #remove start and end times from argument string
    arguments_string = arguments_string.replace(start_time, '')
    arguments_string = arguments_string.replace(end_time, '')

    #sanitize
    start_time = start_time.replace('“','"').replace('”','"')
    start_time = start_time.strip("\"")
    end_time   = end_time.replace('“','"').replace('”','"')
    end_time   = end_time.strip("\"")

    return(query_limit, start_time, end_time, arguments_string)

  def ordered_stocks_parse_users_and_indexes(self, arguments_string):
    """
    Parameters  - arguments_string: str containing the arguments to be parsed

    Return      - user_index_dict: dictionary where k=user_id v=[index_name]. v=None represents selecting all of the user's indexes

    Description - Provided string containing specified discord users and their indexes, tokenize each set and put in user_index_dict dictionary.
    """
    open_parenth_positions  = find(arguments_string, '(')
    close_parenth_positions = find(arguments_string, ')')

    user_index_dict = {} #k=user_id v=[index_names] or None (a None value indicates no specific indexes were provided for k=user_name)
    if len(open_parenth_positions) == 0 and len(close_parenth_positions) == 0:
      """
      No tuples were provided as arguments so each token separated by a comma is assumed to be a discord user 
      """
      discord_user_ids = arguments_string.split(',')
      if len(discord_user_ids) == 0:
        #here
        raise ValueError("Invalid syntax for best/worst stocks query in: %s" % arguments_string.replace('“','`').replace('”','`').replace('\"','`'))
      for user in discord_user_ids:
        user = user.strip()
        user_index_dict[user[3:-1]] = None #
    elif len(open_parenth_positions) == len(close_parenth_positions):
      parenth_pairs = []
      for x in range(len(open_parenth_positions)):
        """
        If we make it here we have an equal amount of open and close parenthesis. This loop will validate their positions and
        will make a pair for the given open and close paranthesis pair.
        """
        if open_parenth_positions[x] < close_parenth_positions[x]:
          parenth_pair = (open_parenth_positions[x], close_parenth_positions[x])
        else:
          raise ValueError("Error: Mismatching parantheses")
        if x > 0:
          prev_pair = parenth_pairs[-1]
          if prev_pair[1] > parenth_pair[0]:
            raise ValueError("Error: Mismatching parantheses")
        parenth_pairs.append(parenth_pair)
      user_index_dict = self.ordered_stocks_tokenize_users_and_indexes(arguments_string, parenth_pairs)
    else:
      raise ValueError("Error: Missing paranthese for `()` pair.")

    return user_index_dict

  def ordered_stocks_tokenize_users_and_indexes(self, func_args, parenth_pairs):
    """
    Parameters  - func_args:
                  parenth_pairs:

    Return      - users_to_indexes: Dictionary where k=user_id, v=[index_name]. Note if no index is specified for a user_id v=None

    Description - This function is a helper parser function for parsing best/worst stocks. Used when index names are specified in conjunction with 
                  a Discord user ID. Provided func_args and an array of tuple pairs containing the index locations of open and close paranthesis, tokenize
                  the users and their corresponding specified indexes to put in dict `users_to_indexes`.
    """
    user_index_parenth_tokens = []
    for parenth_pair in parenth_pairs:
      """
      Given each open/close paranthesis pair, use their positions to tokenize arguments from func_args
      """
      open_offset  = parenth_pair[0]
      close_offset = len(func_args)-1 - parenth_pair[1]
      if close_offset == 0:
        #meaning the last character in all of the provided arguments is a closing paranthesis for a given pair
        user_index_parenth_tokens.append(func_args[open_offset:])
      else:
        user_index_parenth_tokens.append(func_args[open_offset:-close_offset])

    users_to_indexes = {} #dict where k=user_id, v=[indexes]
    tokens_left = func_args
    for parenth_token in user_index_parenth_tokens:
      """
      Two parts to this for loop, for each token enclosed by (), get discord_user_id and index names within
      Remove each token enclosed by () from the overall token string. Then what's left, if anything, are plain user args
      """
      print("the paranthe TOKENNNN: %s" % parenth_token)
      if '[' in parenth_token or ']' in parenth_token:
        """
        (discord_user_id, ["index_name1", "index_name2", ... ])
        below function will raise exception if invalid syntax. bubble it upward and dont bother parsing the rest
        returns tuple of user and array of index names
        """
        (discord_user_id, indexes) = self.ordered_stocks_tokenize_multiple_indexes(parenth_token)

        index_names_no_quotes = []
        for stock_index in indexes:
          index_names_no_quotes.append(stock_index.strip("\""))

        discord_user_id = discord_user_id[3:-1]
        if discord_user_id in users_to_indexes:
          raise ValueError("Error: Provided User, <@%s>, has already been provided as an argument." % discord_user_id)
        users_to_indexes[discord_user_id] = index_names_no_quotes 
      else:
        #just (discord_user_id, "index_name")
        indexes = []
        user_index = parenth_token.split(',')
        if len(user_index) != 2 or len(user_index[0]) == 0 or len(user_index[1]) == 0:
          raise ValueError("Invalid syntax for following token: %s" % parenth_token.replace('“','`').replace('”','`').replace('\"','`'))
        discord_user_id = user_index[0].strip('(').strip()
        discord_user_id = discord_user_id[3:-1]
        indexes.append(user_index[1].strip(')').replace('“','').replace('”','').replace('\"','').strip())
        if discord_user_id in users_to_indexes:
          raise ValueError("Error: Provided User, <@%s>, has already been provided as an argument." % discord_user_id)
        users_to_indexes[discord_user_id] = indexes
      tokens_left = tokens_left.replace(parenth_token, '')

    print("the tokens left: %s" % tokens_left)
    left_overs = tokens_left.split(',')
    for user in left_overs:
      if user.strip().startswith("<@!"):
        if user in users_to_indexes: 
          raise ValueError("Error: Provided User, <@%s>, has already been provided as an argument." % discord_user_id)
        users_to_indexes[user.strip()[3:-1]] = None
    for k, v in users_to_indexes.items():
      print("the k: %s" % k)
    return users_to_indexes

  def ordered_stocks_tokenize_multiple_indexes(self, parenth_token):
    """
    Parameters  - parenth_tokens: tokenized arguments by parenthesis.

    Return      - discord_user_id: The Discord user ID found in the paranthesis token.
                  sanitized_index_names: array containing the names of the specified indexes found in the paranthesis token.

    Description - This function is a helper parser function for parsing best/worst stocks, called by ordered_stocks_tokenize_users_and_indexes. Provided a tokenized
                  argument derived from an open and closing paranthesis, obtain the specified Discord user ID and the list of index names provided.
    """
    sanitized_token = parenth_token.strip('(')
    sanitized_token = sanitized_token.strip(')')
    user_indexes = sanitized_token.split(',', 1)
    if len(user_indexes) != 2:
      raise ValueError("Invalid usage of multiple indexes format in: %s" % parenth_token.replace('“','`').replace('”','`').replace('\"','`'))

    discord_user_id = user_indexes[0].strip()
    if '[' in discord_user_id or ']' in discord_user_id:
      raise ValueError("Invalid Discord User provided as an argument. Nice try ;)")
    bracket_token   = user_indexes[1].strip()
    open_brackets   = find(bracket_token, '[')
    close_brackets  = find(bracket_token, ']')
    if len(open_brackets) != 1 or len(close_brackets) != 1 or open_brackets[0] > close_brackets[0]:
      raise ValueError("Invalid usage of `[]` in token: %s" % parenth_token.replace('“','`').replace('”','`').replace('\"','`'))
    indexes_string = bracket_token.strip('[')
    indexes_string = indexes_string.strip(']')
    index_names = indexes_string.split(',')
    sanitized_index_names = []
    for index_name in index_names:
      index_name = index_name.strip('“')
      index_name = index_name.strip('”')
      index_name = index_name.strip('\'')
      index_name = index_name.strip('\"') #For some reason this is leaving the leading quote on index_names unstripped, hence why callers of this function must do their own sanitization. Strange.
      sanitized_index_names.append(index_name.strip())
    if len(sanitized_index_names) == 0:
      raise ValueError("Invalid Usage of `[]`, no valid index names found.")

    return(discord_user_id, sanitized_index_names)

  def ordered_stocks_get_from_user_index_dict(self, user_index_dict):
    """
    Parameters  - arguments_string: Dictionary containing user_id [index_name] pairs 

    Return      - all_stocks: list containing all the stocks gathered from every specified user_id and index_name.

    Description - Provided dictionary of user_id and index_name pairs, obtain every stock (ignoring duplicates) and place into list `all_stocks` to return 
    """
    stocks_to_users_dict = {}
    all_stocks = []
    for k, v in user_index_dict.items():
      index_names = []
      if v is None:
        index_names = self.market_api.get_user_indexes(k)
      else:
        #set index_names as None to indicate getting all index names
        index_names = v
      if index_names is None or len(index_names) == 0:
        continue
      for index_name in index_names:
        tickers = self.market_api.get_index_tickers(k, index_name)
        if tickers is None or len(tickers) == 0:
          continue
        for ticker in tickers:
          if ticker not in all_stocks:
            all_stocks.append(ticker)
          if ticker in stocks_to_users_dict:
            if k in stocks_to_users_dict[ticker]:
              continue
            else:
              stocks_to_users_dict[ticker].append(k)
          else:
            users_array = []
            users_array.append(k)
            stocks_to_users_dict[ticker] = users_array

    if len(all_stocks) == 0:
      raise ValueError("No stocks were found with provided users and indexes.")
    return all_stocks

  def ordered_stocks_get_message(self, sorted_returns, function_id, query_limit):
    """
    Parameters  - sorted_returns: Dictionary where k=ticker, v=return in sorted order (from greatest return to worst) 
                  function_id:    int of value 2 or 3 representing get best or get worst stocks respectively.
                  query_limit:    int representing the limit of stocks to obtain the best/worst returns for.

    Return      - str_msg: Formatted message displaying the best/worst stocks and their returns.

    Description - Provided dictionary of sorted returns and query limit, derive a formatted message to be sent up and displayed on the originating Discord channel.
    """
    pair_count = len(sorted_returns)
    sub_dict = {}

    subset_count = query_limit
    for k, v in sorted_returns.items():
      if function_id == 3:
        if subset_count > 0:
          sub_dict[k] = v
          subset_count = subset_count - 1
        else:
          break
      else:
        if pair_count <= query_limit:
          sub_dict[k] = v
        pair_count = pair_count - 1

    str_msg = ""
    for k, v in sub_dict.items():
      tick_len = len(k)
      spacing = ""
      for x in range(5 - tick_len):
        spacing = spacing + " "
      if function_id == 3:
        str_msg = str_msg + "Stock: `%s%s`--- Return: `%4f%s`\\n" % (k, spacing, v, "%")
      else:
        str_msg =  "Stock: `%s%s`--- Return: `%4f%s`\\n" % (k, spacing, v, "%") + str_msg

    if function_id == 3:
      str_msg = "Top %d Worst Stocks:\\n" % query_limit + str_msg
    else:
      str_msg = "Top %d Best Stocks:\\n" % query_limit + str_msg
    
    return str_msg

  def parse_cron_job_arg(self, arg):
    """
    Parameteres  - arg:  str containing a single comma separated token derived from `add_cron_job()` argument string

    Return       - pair containing the argument type and its corresponding value. 

    Description  - Provided arg str, tokenize by `=` and determine if the l-value is a valid argument type. If so return it along with its corresponding specified value.
    """
    l_r_vals = arg.split('=')
    if len(l_r_vals) != 2:
      raise ValueError("Invalid Args: %s." % arg)
    l_val = l_r_vals[0].strip()
    r_val = l_r_vals[1].strip()

    if l_val == 'script':
      script_name = r_val.strip('\'')
      script_name = script_name.strip('\"')
      if script_name in self.uploaded_mods:
        return(l_val, script_name)
      else:
        raise ValueError("Provided script name, `%s`, does not correspond with an existing script." % script_name)
    elif l_val == 'year':
      year_val = r_val.strip("\'")
      return (l_val, year_val.strip("\""))
    elif l_val == 'month':
      month_val = r_val.strip("\'")
      return (l_val, month_val.strip("\""))
    elif l_val == 'day_of_week':
      dow_val = r_val.strip("\'")
      return (l_val, dow_val.strip("\""))
    elif l_val == 'hour':
      hour_val = r_val.strip("\'")
      return (l_val, hour_val.strip("\""))
    elif l_val == 'minute':
      minute_val = r_val.strip("\'")
      return (l_val, minute_val.strip("\""))
    else:
      raise ValueError("Invalid l_val.")

  """
  Script and file related functions.
  """
  def run_script(self, **kwargs):
    script_name = kwargs['name']
    user_id = kwargs['user_id']
    channel_id = kwargs['channel_id']
    if script_name in self.uploaded_mods:
      if isinstance(self.uploaded_mods[script_name], str):
        self.run_se_script(self.uploaded_mods[script_name], user_id, channel_id)
      else:
        market_script_obj = self.uploaded_mods[script_name]
        with self.mutex:
          """
          Since parse_and_run_market_cmd can be spawned as a separate thread with multuple concurrently running, the thread_count needs to be protected
          """
          market_obj = market_script_obj(thread_ID=self.thread_count, thread_name="%s" % script_name + str(self.thread_count), parent_queue=self.script_results_queue, market_api=self.market_api)
          self.threadID_to_channelID[self.thread_count] = channel_id
          self.thread_count = self.thread_count + 1
        market_obj.start()
    else:
      json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "MarketBehaviorSet: Script, `%s`, was not found at the time of cron task execution." % script_name)
      self.parent_queue.put(json_obj)

  def run_se_script(self, se_script, user_id, channel_id):
    """
    Parameteres  - se_script:  str containing file path to the .se file to run
                   user_id:    str containing the Discord user ID of command sender
                   channel_id: str containing the Discord channel ID from which the command was sent from

    Return       - void

    Description  - Open and read the specified .se file to run and execute each ./market command within linearly.
    """
    se_file = open(se_script, "r")
    with open(se_script) as f:
      script = f.readlines()

    script = [line.strip() for line in script] 
    for cmd in script:
      #not spawned as separate thread for each command unlike direct ./market command inputs to discord channels
      #needs to run linearly so that commands occur in order 
      self.parse_and_run_market_cmd(cmd, user_id, channel_id)
    return

  def handle_file_write(self, user_id, channel_id, line):
    """
    Parameters  - user_id:    str of Discord user ID
                  channel_id: str of Discord channel ID
                  line:       str containing stock ticker or EOF

    Return      - JSON object containing status of line parsed

    Description - Handles a single line of input provided by a Discord user currently writing to an open index. Expects input to be a valid ticker or EOF indicator.
                  If not EOF determine if input is a valid ticker by querying the market_api's validate ticker functionality. If ticker is valid and not already entered
                  then append to the list containing the entered tickers (self.open_index_files[user_id][1]).
                  If EOF indicator is met then write entered tickers from the list to a .csv in format 1 (See market_api.py's README for formats) and then create the
                  index/portfolio for the user. Return the results as a JSON object to be pushed back onto the originating Discord channel.
    """
    if "\n" in line:
      print("here1")
      json_obj ='{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: Ticker entries must be 1-by-1 on single lines. Ignoring given input: %s" % line)
      return json_obj
    elif ":w" == line.strip(): #EOF indicator
      index_name = self.open_index_files[user_id][0]
      ticker_entries = self.open_index_files[user_id][1]
      total_tickers = len(ticker_entries)
      if total_tickers == 0:
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "No valid tickers were entered. Index `%s` will not be created." % index_name)
        del self.open_index_files[user_id]
        return json_obj
      else:
        ticker_entries.insert(0,"ticker")
        market_scripts_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "market_scripts")
        if not os.path.exists(market_scripts_path):
          os.makedirs(market_scripts_path)

        uploads_path = os.path.join(market_scripts_path, "uploads")
        if not os.path.exists(uploads_path):
          os.makedirs(uploads_path)
        
        full_path = os.path.join(uploads_path, index_name.replace(" ","_"))
        full_path = full_path + ".csv"
        file = open(full_path, "w")
        file.write('\n'.join(ticker_entries))


        rval = self.market_api.create_portfolio(user_id, '\n'.join(ticker_entries), index_name)


        del self.open_index_files[user_id]
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s", "file_path":"%s"}' % (channel_id, "text/upload", rval, full_path)
        return json_obj
    else:
      ticker = self.market_api.validate_ticker(line)
      if ticker:
        if ticker in self.open_index_files[user_id][1]:
          error_msg = "Error: Ticker `$%s` has already been entered. Skipping..." % ticker
          json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", error_msg)
          return json_obj

        self.open_index_files[user_id][1].append(ticker)
        return None
      else:
        error_msg = "Error: Ticker `%s` is not a valid ticker. Skipping..." % line.strip()
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", error_msg)
        return json_obj

  def handle_se_script_write(self, user_id, channel_id, market_cmd):
    """
    Parameters  - user_id:    str of Discord user ID
                  channel_id: str of Discord channel ID
                  market_cmd: str of ./market command to write

    Return      - JSON object containing the status of the handling of input

    Description - Handles a single line of input provided by a Discord user currently writing to an open se script. Expects input to be a ./market command or EOF indicator.
                  If not EOF then append the line to 'self.open_se_scripts' (validity of command will be determined upon running the script), else write the lines inserted in
                  'open_se_scripts' to a new file to be placed in '/behavior_sets/se_scripts' (overwrite .se file if already exists).
    """
    if "\n" in market_cmd:
      json_obj ='{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "Error: Market commands must be 1-by-1 on single lines. Ignoring given input: %s" % market_cmd)
      return json_obj
    elif ":w" == market_cmd.strip():
      se_script_name = self.open_se_scripts[user_id][0]
      market_cmds = self.open_se_scripts[user_id][1]
      total_cmds = len(market_cmds)
      if total_cmds == 0:
        json_obj = '{"channel":%d, "data_type":"%s", "data":"%s"}' % (channel_id, "text_message", "No valid market commands were entered. SE script, `%s`, will not be created." % se_script_name )
        del self.open_se_scripts[user_id]
        return json_obj
      se_scripts_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'se_scripts')
      if not os.path.exists(se_scripts_path):
        os.makedirs(se_scripts_path)
      full_path = os.path.join(se_scripts_path, se_script_name.replace(" ","_"))
      full_path = full_path + ".se"

      overwrite = "wrote"
      if os.path.exists(full_path):
        overwrite = "overwrote"
        os.remove(full_path)

      file = open(full_path, "w")
      file.write('\n'.join(market_cmds))
      del self.open_se_scripts[user_id]
      self.uploaded_mods[se_script_name] = full_path
      success_msg = "Successfully %s and uploaded se script, `%s`, for <@%s>" % (overwrite, se_script_name.replace(' ','_'), user_id)
      json_obj = '{"channel":%d, "data_type":"%s", "data":"%s", "file_path":"%s"}' % (channel_id, "text/upload", success_msg, full_path)
      return json_obj
    else:
      self.open_se_scripts[user_id][1].append(market_cmd.strip())

  def handle_se_script_upload(self, se_url):
    """
    Parameters  - se_url: URL str of .se file to upload

    Return      - str: Status message
    Description - Get .se file data from the URL request then sanitize and write the .se file data to a new file to /behavior_sets/se_scripts.
                  If successful map (self.uploaded_mods) .se file name to the path it was written to.
    """
    base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "se_scripts")
    if not os.path.exists(base_path):
      os.makedirs(base_path)
    file_name = path_leaf(se_url)
    try:
      #fudge url here to verify behavior
      r = requests.get(url = se_url)
    except requests.exceptions.RequestException as e:
      raise ValueError("Bad URL for .se file uploaded. Contact admin.")

    full_path = os.path.join(base_path, file_name)
    if os.path.exists(full_path):
      os.remove(full_path)
    
    file = open(full_path, "w")
    lines = []
    line = ""
    for char in r.text:
      if char != "\n":
        line = line + char
      else:
        if line == "":
          continue
        else:
          lines.append(line)
          line = ""
    if line != "":
      lines.append(line)
    file.write(''.join(lines))
    file.close()

    self.uploaded_mods[file_name[:-3]] = full_path
    return "Successfully uploaded: `%s`!" % file_name

  def handle_script_upload(self, python_file_uploads):
    """
    Parameters  - python_file_uploads: List of urls to uploaded .py files

    Return      - market_script_class_name: str the name of the MarketScript implementation
                  market_script:            Class object of the MarketScript implementation (careful to note it is not an instantiation)

    Description - For each python url in 'python_file_uploads' retrieve the data and sanitize and write it to a newly created file.
                  Determine if the module has been uploaded before, if so set the 'reload_mod' flag. Import every Python file via
                  'self.is_class_market_script'. If exactly one of the modules uploaded contains an implementation of a MarketScript
                  then 'is_class_market_script' will return the class object (not an instantiation of the MarketScript class object).   
    """
    base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "market_scripts")
    if not os.path.exists(base_path):
      os.makedirs(base_path)
    valid_py_uploads = {}
    for py_file in python_file_uploads:
      print("before path leaf: %s" % py_file)
      file_name = path_leaf(py_file)
      print("after path leaf: %s" % file_name)
      try:
        r = requests.get(url=py_file)
      except Exception as e:
        print("Failed requesting url: " + py_file + "\n" + str(e))
        raise ValueError("Failed to upload file: %s" % file_name)
      else:
        valid_py_uploads[file_name] = r
    #below will hold the uploaded market_script implementation as a class object, if valid
    market_script = None
    market_script_class_name = ""
    #write every python file uploaded to ./market_scripts/ and note the MarketScript implementation. Throw error if more than one
    for k, v in valid_py_uploads.items():
      """
      For each python file uploaded sanitize the file and upload to '/behavior_sets/market_scripts/'. 
      Determine if a Python file already exists with that name, if so set reload_mod flag to overwrite.
      Import each Python module and determine which one is an implementation of a MarketScript and obtain its class name and class object to return.
      If more than one Python module is uploaded then it is assumed there exists only one MarketScript implementation wherein the remainder are module dependencies
      """
      reload_mod = False
      full_path = os.path.join(base_path, k)
      if os.path.exists(full_path):
        os.remove(full_path)
        reload_mod = True

      file = open(full_path, "w")
      lines = []
      line = ""
      for char in v.text:
        """
        For loop to sanitize the input to be more readable
        """
        if char != "\n":
          line = line + char
        else:
          if line == "":
            continue
          else:
            lines.append(line)
            line = ""
      if line != "":
        lines.append(line)
      file.write(''.join(lines))
      file.close()

      market_script_obj = self.is_class_market_script('behavior_sets.market_scripts.' + k[:-3], k[:-3], reload_mod)
      if market_script_obj:
        if market_script is not None:
          raise ValueError("Error, more than one file of the files uploaded contained an implementeation of a MarketScript.")
        else:
          market_script = market_script_obj
          market_script_class_name = k[:-3]
      else:
        print("Debug: this is not the module we're interested in: %s" % k)
        raise ValueError("Was unable to obtain a MarketScript obj from the provided module.")
    
    return (market_script_class_name, market_script)
    
  def is_class_market_script(self, mod_abs_path, mod_name, reload_mod):
    """
    Parameters  - mod_abs_path: str path to uploaded module
                  mod_name:     str Name of the module
                  reload_mod:   Boolean to reload already imported module
    Description - Import module, unless it is already imported then reload, then determine if module contains a MarketScript implementation or if it is a module dependency. 
                  Establish mapping between module name and imported mod object. If the imported mod contains a subclass of MarketScript return the class obj in the module.
    """
    if reload_mod and mod_name in self.script_name_to_mod:
      print("We out here")
      mod = self.script_name_to_mod[mod_name]
      reload(mod)
    else:
      mod = import_module(mod_abs_path)
      self.script_name_to_mod[mod_name] = mod
    for name, obj in inspect.getmembers(mod):
      if inspect.isclass(obj):
        if issubclass(obj, MarketScript):
          if name != "MarketScript":
            return obj
        else:
          attributes = inspect.getmembers(obj, lambda a:not(inspect.isroutine(a)))
          isBase = [a for a in attributes if a[0].startswith('isBase')]
          if len(isBase) == 1:
            if isBase[0][1] is False:
              print(isBase[0][1])
              return obj


    return None

"""
Helper functions
"""
def path_leaf(path):
  head, tail = ntpath.split(path)
  return tail or ntpath.basename(head)

def find(s, ch):
  """
  returns an array of positions in s where ch occurs
  """
  return [i for i, ltr in enumerate(s) if ltr == ch]




