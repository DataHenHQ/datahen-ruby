require 'concurrent'
require 'parallel'

module Datahen
  module Scraper
    class BatchParser
      NOT_FOUND_MSG = "No more pages to parse found"
      NO_DEQUEUE_COUNT_MSG = "\nWarning: Max page to parse dequeue count is 0, check pages to parse scale\n"
      NO_WORKERS_MSG = "\nWarning: There are no parser workers\n"

      # Configuration file path.
      # @return [String] config file path
      attr_accessor :config_file
      # Garbage collector request counter.
      # @return [Integer] garbage collector counter
      attr_accessor :garbage_count
      # Last printed message, useful to prevent duplicated log messages.
      # @return [String] last printed message
      attr_accessor :last_message
      # Second dequeue counter used to prevent false negative warning messages.
      # @return [Integer] second dequeue counter
      attr_accessor :second_dequeue_count
      # Dequeue API request timeout in seconds.
      # @return [Integer] dequeue API request timeout in seconds
      attr_accessor :dequeue_timeout
      # Job id to be executed.
      # @return [Integer] job id
      attr_reader :job_id
      # Parallel worker quantity.
      # @return [Integer] parallel worker quantity
      attr_reader :worker_count
      # Loaded pages array.
      # @return [Concurrent::Array<Hash>] loaded pages as an array
      attr_reader :pages
      # Loaded pages hash, useful to avoid duplicates on the loaded pages array.
      # @return [Concurrent::Hash<String, Hash>] loaded pages as a concurrent hash
      attr_reader :loaded_pages
      # Max garbage collector requests before actually executing the garbage
      #   collector.
      # @return [Integer] max garbage request quantity before actually executing
      #   it
      attr_reader :max_garbage
      # Dequeue interval in seconds.
      # @return [Integer] dequeue interval in seconds
      attr_reader :dequeue_interval
      # Dequeue scale used to calculate the ideal dequeue size.
      # @return [Numeric] dequeue scale
      attr_reader :dequeue_scale
      # Known page types extracted from the config file.
      # @return [Array<String>] known page types
      attr_reader :page_types
      # Known parsers extracted from the config file.
      # @return [Concurrent::Hash<String, String>] known parsers
      attr_reader :parsers
      # Current config file loaded.
      # @return [Hash] current loaded configuration
      attr_reader :config
      # Datahen job pages client used for API pages dequeuing.
      # @return [Datahen::Client::JobPage] datahen job pages API client
      attr_reader :client
      # Garbage collector mutex used to synchronize garbage collector requests.
      # @return [Mutex] garbage collector mutex
      attr_reader :garbage_mutex
      # Current dequeuer thread.
      # @return [Thread] dequeuer thread
      attr_reader :dequeuer_thread
      # Dequeuer mutext used to synchronize page dequeuing.
      # @return [Mutex] dequeuer mutex
      attr_reader :dequeue_mutex
      # Dequeuer last run unix timestamp.
      # @return [Integer] dequeuer last run unix timestamp
      attr_reader :dequeuer_still_alive
      # Indicates whenever the wait time is because there are no more pages.
      # @return [Boolean] `true` when wait time is due to no more pages,
      #   else `false`
      attr_reader :not_found

      # Wait a specific amount of seconds.
      # @param [Integer] time_in_seconds Seconds to wait.
      def self.wait time_in_seconds
        Kernel.sleep time_in_seconds
      end

      # Get a unix timestamp.
      # @return [Integer] unix timestamp
      def self.timestamp
        Time.new.utc.to_i
      end

      # Initialize a batch parser object.
      # @param [Integer] job_id Job id.
      # @param [String] config_file Config file path.
      # @param [Hash] opts ({}) Configuration options
        # @option opts [Integer] :worker_count (1) Parallel worker quantity.
        # @option opts [Integer] :max_garbage (5) Max amount of times the garbage
        #   collector can be requested before actually executing.
        # @option opts [Integer] :dequeue_interval (3) Time in seconds to wait
        #   between page dequeuing.
        # @option opts [Numeric] :dequeue_scale (2) Scaling factor to used to
        #   calculate page dequeue size.
        # @option opts [Numeric] :dequeue_timeout (30) Page dequeue API request
        #   timeout in seconds.
        # @option opts [Hash] :client_options ({}) Datahen client gem additional
        #   options (see Datahen::Client::Base#initialize method).
      def initialize(job_id, config_file, opts = {})
        opts = {
          worker_count: 1,
          max_garbage: 5,
          dequeue_interval: 3,
          dequeue_scale: 2,
          dequeue_timeout: 30,
          client_options: {}
        }.merge opts

        @job_id = job_id
        @worker_count = opts[:worker_count]
        @dequeue_interval = opts[:dequeue_interval]
        @dequeue_scale = opts[:dequeue_scale]
        @max_garbage = opts[:max_garbage]
        @pages = Concurrent::Array.new
        @loaded_pages = Concurrent::Hash.new
        @garbage_mutex = Mutex.new
        @dequeue_mutex = Mutex.new
        @not_found = false
        self.dequeue_timeout = opts[:dequeue_timeout]
        self.second_dequeue_count = 0
        self.garbage_count = 0
        self.config_file = config_file
        self.load_config

        @client = Datahen::Client::JobPage.new(opts[:client_options])
        nil
      end

      # Execute garbage collector after it is requested as many times as
      #   described by #max_garbage.
      def recollect_garbage
        self.garbage_mutex.synchronize do
          self.garbage_count += 1
          if self.garbage_count > self.max_garbage
            puts "Recollect garbage"
            GC.start
            self.garbage_count = 0
          end
        end
        nil
      end

      # Loads the config file into a Hash.
      def load_config
        # build page type to script file map
        @page_types = []
        @parsers = Concurrent::Hash.new
        @config = YAML.load_file(config_file)
        (self.config['parsers'] || []).each do |v|
          next if !v['disabled'].nil? && !!v['disabled']
          @page_types << v['page_type']
          self.parsers[v['page_type']] = v['file']
        end
        self.recollect_garbage
        nil
      end

      # Print the message regardless of it being the same as the last message.
      # @param [String] message Message to display.
      def repeat_puts message
        puts message
        self.last_message = message
        nil
      end

      # Print the message only when it is different from the last recorded
      #   message.
      # @param [String] message Message to display.
      def no_repeat_puts message
        return if message == self.last_message
        puts message
        self.last_message = message
        nil
      end

      # Refresh dequeuer's still alive timestamp
      def dequeuer_is_alive!
        self.dequeue_mutex.synchronize do
          @dequeuer_still_alive = self.class.timestamp
        end
        nil
      end

      # Load new pages by dequeuing from the API.
      # @return [Integer] amount of pages loaded
      def load_pages
        self.dequeuer_is_alive!

        # calculate dequeue size
        max_dequeue_size = (self.worker_count * self.dequeue_scale).ceil
        current_size = self.pages.length
        dequeue_size = (self.dequeue_scale * (max_dequeue_size - current_size)).ceil
        if dequeue_size < 1
          return 0
        end
        dequeue_size = max_dequeue_size if dequeue_size > max_dequeue_size

        # reserve and get to pages parse
        response = nil
        begin
          response = client.dequeue self.job_id,
            dequeue_size,
            self.page_types,
            config['parse_fetching_failed'],
            timeout: self.dequeue_timeout
        rescue Net::ReadTimeout, Net::OpenTimeout => e
          self.repeat_puts "Dequeue API call timeout! Contact infra team, your job needs a profile change"
          self.dequeuer_is_alive!
          return 0
        rescue => e
          raise e
        end
        self.dequeuer_is_alive!

        # ensure a valid response or try again
        if response.body.nil? || response.body.empty? || response.response.code.to_i != 200
          self.repeat_puts(response.nil? ? 'null' : response.body)
          self.recollect_garbage
          return 0
        end

        # add pages
        count = 0
        json = JSON.parse(response.body)
          if json['error'] != ""
            return 0
          end
        (json['data'] || []).each do |page|
          count += 1
          next if self.loaded_pages.has_key? page['gid']
          self.pages << (self.loaded_pages[page['gid']] = page)
        end
        response = nil
        self.dequeuer_is_alive!

        # recolect garbage to free some memory before parsing
        if count > 0
          @not_found = false
          self.recollect_garbage
          self.repeat_puts "Found #{count} page(s) to parse"
          self.second_dequeue_count += 1 unless self.second_dequeue_count > 1
        else
          @not_found = true
          self.no_repeat_puts NOT_FOUND_MSG
        end

        # return how many pages were loaded
        count
      end

      # Ensures that the dequeuer thread exists and is running.
      # @return [Boolean] `true` if thread was alive, or `false` if had to
      #   create a new thread
      def ensure_dequeuer_thread
        self.dequeue_mutex.synchronize do
          # check if dequeuer thread is alive and healthy
          if !self.dequeuer_thread.nil? && self.dequeuer_thread.alive?
            still_alive_timeout = (self.dequeue_timeout + self.dequeue_interval) * 2 + self.dequeuer_still_alive
            return true if self.class.timestamp < still_alive_timeout

            # kill dequeuer thread
            self.repeat_puts "Dequeuer isn't healthy, will restart it..."
            self.dequeuer_thread.kill
            @dequeuer_thread = nil
            self.recollect_garbage
            self.no_repeat_puts "Dequeuer thread was killed!"
          end

          # dequeuing on parallel (the ride never ends :D)
          @dequeuer_thread = Thread.new do
            while true
              begin
                self.load_pages
                self.class.wait self.dequeue_interval
              rescue => e
                puts [e.message] + e.backtrace rescue 'error'
              end
            end
            puts "Error: dequeuer died! D:"
          end
          self.repeat_puts "Dequeuer thread was started!"
        end
        false
      end

      # Dequeue one page from the previously loaded pages, and waits until there
      #   are new pages whenever there are no loaded pages.
      # @return [Hash] dequeued page
      def dequeue_pages
        # collect garbage
        self.recollect_garbage

        # return page if there are loeaded pages
        is_waiting = false
        while true do
          page = self.pages.shift
          unless page.nil?
            puts "[Worker #{Parallel.worker_number}]: Finish waiting" if is_waiting
            loaded_pages.delete(page['gid'])
            return page
          end

          # be more verbose on worker waiting
          unless is_waiting
            is_waiting = true
            puts "[Worker #{Parallel.worker_number}]: Is waiting for a page..."
            if self.second_dequeue_count > 1 && !self.not_found
              puts "\nWARNING: Your job might not be optimized. Consider increasing your job's \"parser_dequeue_scale\" if the `to_parse` queue is not empty or near empty \n"
            end
          end
          self.class.wait 1

          # ensure the dequeuer thread is alive and healthy
          self.ensure_dequeuer_thread
        end
      end

      # Dequeue pages and execute the parsers associated to them on parallel.
      def exec_parse save = false, keep_outputs = false
        if self.worker_count < 1
          self.no_repeat_puts NO_WORKERS_MSG
          return
        else
          self.no_repeat_puts "Spawing #{self.worker_count} workers"
        end

        # start dequeuer
        self.ensure_dequeuer_thread

        # process the pages
        dequeue = lambda{ self.dequeue_pages }
        Parallel.each(dequeue, in_threads: (worker_count)) do |page|
          parser_file = self.parsers[page['page_type']]
          begin
            self.repeat_puts("Parsing page with GID #{page['gid']}")
            puts Datahen::Scraper::Parser.exec_parser_by_page(
              parser_file,
              page,
              job_id,
              save,
              nil,
              keep_outputs
            )
            self.repeat_puts("Finish parsing page with GID #{page['gid']}")
          rescue Parallel::Kill => e
            puts "[Worker #{Parallel.worker_number}]: Someone tried to kill Parallel!!!"
          rescue Parallel::Break => e
            puts "[Worker #{Parallel.worker_number}]: Someone tried to break Parallel!!!"
          rescue => e
            puts [e.message] + e.backtrace rescue 'error'
          end
        end

        nil
      end
    end
  end
end
