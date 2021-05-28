require 'concurrent'
require 'parallel'

module Datahen
  module Scraper
    class BatchParser
      NOT_FOUND_MSG = "No more pages to parse found"
      NO_DEQUEUE_COUNT_MSG = "\nWarning: Max page to parse dequeue count is 0, check pages to parse scale\n"
      NO_WORKERS_MSG = "\nWarning: There are no parser workers\n"

      attr_accessor :config_file, :garbage_count, :last_message, :second_dequeue_count
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
        self.second_dequeue_count = 0
        self.garbage_count = 0
        self.config_file = config_file
        self.load_config

        @client = Datahen::Client::JobPage.new(opts[:client_options])
      end

      def recollect_garbage
        self.garbage_mutex.synchronize do
          self.garbage_count += 1
          if self.garbage_count > self.max_garbage
            puts "Recollect garbage"
            GC.start
            self.garbage_count = 0
          end
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
        response = nil
        begin
          response = client.dequeue self.job_id,
            dequeue_size,
            self.page_types,
            config['parse_fetching_failed']
        rescue Net::ReadTimeout, Net::OpenTimeout => e
          self.no_repeat_puts "Dequeue API call timeout! Contact infra team, your job needs a profile change"
          return 0
        rescue => e
          raise e
        end

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
          self.second_dequeue_count += 1 unless self.second_dequeue_count > 1
        else
          self.no_repeat_puts NOT_FOUND_MSG
        end

        # return how many pages were loaded
        count
      end

      def dequeue_pages
        # collect garbage
        self.recollect_garbage

        # return page if there are loeaded pages
        is_waiting = false
        while true do
          key_value = self.pages.shift
          unless key_value.nil?
            puts "[Worker #{Parallel.worker_number}]: Finish waiting" if is_waiting
            return key_value[1]
          end

          # be more verbose on worker waiting
          unless is_waiting
            is_waiting = true
            puts "[Worker #{Parallel.worker_number}]: Is waiting for a page..."
            if self.second_dequeue_count > 1
              puts "\nWARNING: Your job is not optimized, increase your job's \"parser_dequeue_scale\"\n"
            end
          end
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

        # dequeuing on parallel (the ride never ends :D)
        Thread.new do
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

        # process the pages
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
          rescue Parallel::Kill => e
            puts "[Worker #{Parallel.worker_number}]: Someone tried to kill Parallel!!!"
          rescue Parallel::Break => e
            puts "[Worker #{Parallel.worker_number}]: Someone tried to break Parallel!!!"
          rescue => e
            puts [e.message] + e.backtrace rescue 'error'
          end
        end
      end
    end
  end
end
