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
      attr_reader :config, :client

      def recollect_garbage
        puts "Recollect garbage"
        GC.start
        self.garbage_count = 0
      end

      def self.wait time_in_seconds
        Kernel.sleep time_in_seconds
      end

      def initialize(job_id, config_file, opts = {})
        opts = {
          worker_count: 1,
          max_garbage: 5,
          dequeue_interval: 1,
          dequeue_scale: 1.5,
          client_options: {}
        }.merge opts

        @job_id = job_id
        @worker_count = opts[:worker_count]
        @dequeue_interval = opts[:dequeue_interval]
        @dequeue_scale = opts[:dequeue_scale]
        @max_garbage = opts[:max_garbage]
        @pages = []
        self.garbage_count = 0
        self.config_file = config_file
        self.load_config

        @client = Datahen::Client::JobPage.new(opts[:client_options])
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
        dequeue_size = (self.worker_count * self.dequeue_scale).ceil - self.pages.length
        if dequeue_size < 1
          self.no_repeat_puts NO_DEQUEUE_COUNT_MSG
          return 0
        end
        response = client.dequeue self.job_id,
          dequeue_size,
          self.page_types,
          config['parse_fetching_failed']
        #response = Client::JobPage.new(per_page: dequeue_size, status: 'to_parse', page: 1).all(self.job_id)

        # ensure a valid response or try again
        if response.nil? || response.response.code.to_i != 200
          self.repeat_puts(response.nil? ? 'null' : response.body)
          self.recollect_garbage
          return 0
        end

        # add pages
        count = 0
        (JSON.parse(response.body) || []).each do |page|
          self.pages << page
          count += 1
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
        return self.pages.shift if self.pages.length > 0

        # keep trying to load new pages from API whenever the queue is empty
        while self.load_pages < 1
          self.class.wait self.dequeue_interval
        end

        #Parallel::Stop
        self.pages.shift
      end

      def exec_parse save = false, keep_outputs = false
        if self.worker_count < 1
          self.no_repeat_puts NO_WORKERS_MSG
          return
        else
          self.no_repeat_puts "Spawing #{self.worker_count} workers"
        end
        dequeue = lambda{ self.dequeue_pages }
        Parallel.each(dequeue, in_threads: worker_count) do |page|
          parser_file = self.parsers[page['page_type']]
          puts Datahen::Scraper::Parser.exec_parser_by_page(
            parser_file,
            page,
            job_id,
            save,
            nil,
            keep_outputs
          )
        end
      end
    end
  end
end
