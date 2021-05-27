require 'concurrent'
require 'parallel'

module Datahen
  module Scraper
    class BatchParser
      NOT_FOUND_MSG = "No more pages to parse found"
      NO_DEQUEUE_COUNT_MSG = "Warning: Max page to parse dequeue count is 0, check pages to parse scale"
      NO_WORKERS_MSG = "Warning: There are no parser workers"

      attr_accessor :config_file, :garbage_count, :last_message
      attr_reader :job_id, :worker_count, :pages, :max_garbage
      attr_reader :dequeue_interval, :dequeue_scale
      attr_reader :page_types, :parsers
      attr_reader :config, :client, :garbage_mutex

      def self.wait time_in_seconds
        Kernel.sleep time_in_seconds
      end

      def initialize(job_id, config_file, opts = {})
        opts = {
          worker_count: 1,
          max_garbage: 5,
          dequeue_interval: 3,
          dequeue_scale: 2,
          client_options: {}
        }.merge opts

        @job_id = job_id
        @worker_count = opts[:worker_count]
        @dequeue_interval = opts[:dequeue_interval]
        @dequeue_scale = opts[:dequeue_scale]
        @max_garbage = opts[:max_garbage]
        @pages = Concurrent::Hash.new
        @garbage_mutex = Mutex.new
        self.garbage_count = 0
        self.config_file = config_file
        self.load_config

        @client = Datahen::Client::JobPage.new(opts[:client_options])
      end

      def recollect_garbage
        self.garbage_mutex.synchronize do
          puts "Recollect garbage"
          GC.start
          self.garbage_count = 0
        end
      end

      def load_config
        # build page type to script file map
        @page_types = []
        @parsers = Concurrent::Hash.new
        @config = YAML.load_file(config_file)
        self.config['parsers'].each do |v|
          next if !v['disabled'].nil? && !!v['disabled']
          @page_types << v['page_type']
          self.parsers[v['page_type']] = v['file']
        end
        self.recollect_garbage
      end

      def repeat_puts message
        puts message
        self.last_message = ''
      end

      def no_repeat_puts message
        return if message == self.last_message
        puts message
        self.last_message = message
      end

      def load_pages
        # calculate dequeue size
        max_dequeue_size = (self.worker_count * self.dequeue_scale).ceil
        current_size = self.pages.length
        dequeue_size = (self.dequeue_scale * (max_dequeue_size - current_size)).ceil
        if dequeue_size < 1
          return 0
        end
        dequeue_size = max_dequeue_size if dequeue_size > max_dequeue_size

        # reserve and get to pages parse
        response = client.dequeue self.job_id,
          dequeue_size,
          self.page_types,
          config['parse_fetching_failed']

        # ensure a valid response or try again
        if response.nil? || response.response.code.to_i != 200
          self.repeat_puts(response.nil? ? 'null' : response.body)
          self.recollect_garbage
          return 0
        end

        # add pages
        count = 0
        (JSON.parse(response.body) || []).each do |page|
          count += 1
          next if self.pages.has_key? page['gid']
          self.pages[page['gid']] = page
        end
        response = nil

        # recolect garbage to free some memory before parsing
        if count > 0
          self.recollect_garbage
          self.repeat_puts "Found #{count} page(s) to parse"
        else
          self.no_repeat_puts NOT_FOUND_MSG
        end

        # return how many pages were loaded
        count
      end

      def dequeue_pages
        # collect garbage
        self.garbage_count += 1
        if self.garbage_count > self.max_garbage
          self.recollect_garbage
        end

        # return page if there are loeaded pages
        while true do
          key_value = self.pages.shift
          return key_value[1] unless key_value.nil?
          self.class.wait 1
        end
      end

      def exec_parse save = false, keep_outputs = false
        if self.worker_count < 1
          self.no_repeat_puts NO_WORKERS_MSG
          return
        else
          self.no_repeat_puts "Spawing #{self.worker_count} workers"
        end

        # dequeuing on parallel
        keep_dequeue = Concurrent::Array.new
        keep_dequeue[0] = true
        Thread.new do
          while keep_dequeue[0]
            begin
              self.load_pages
              self.class.wait self.dequeue_interval
            rescue Exception => e
              puts [e.message] + e.backtrace
            end
          end
        end

        dequeue = lambda{ self.dequeue_pages }
        Parallel.each(dequeue, in_threads: (worker_count)) do |page|
          parser_file = self.parsers[page['page_type']]
          begin
            puts Datahen::Scraper::Parser.exec_parser_by_page(
              parser_file,
              page,
              job_id,
              save,
              nil,
              keep_outputs
            )
          rescue Exception => e
            puts [e.message] + e.backtrace
          end
        end
        keep_dequeue[0] = false
      end
    end
  end
end
