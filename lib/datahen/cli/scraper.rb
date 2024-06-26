module Datahen
  class CLI < Thor
    class Scraper < Thor

      desc "list", "List scrapers"
      long_desc <<-LONGDESC
        List all scrapers.
      LONGDESC
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :status, :aliases => :s, type: :string, desc: 'Scraper status. Status can be: done, cancelled, paused, finishing.'
      def list
        client = Client::Scraper.new(options)
        puts "#{client.all}"
      end

      desc "create <scraper_name> <git_repository>", "Create a scraper"
      long_desc <<-LONGDESC
          Creates a scraper\x5
          <scraper_name>: Scraper name can only consist of alphabets, numbers, underscores and dashes. Name must be unique to your account.\x5
          <git_repository>: URL to a valid Git repository.\x5
          LONGDESC
      option :branch, :aliases => :b, desc: 'Set the Git branch to use. Default: master'
      option :freshness_type, :aliases => :t, desc: 'Set how fresh the page cache is. Possible values: day, week, month, year. Default: any'
      option :proxy_type, desc: 'Set the Proxy type. Default: standard'
      option :force_fetch, :aliases => :f, type: :boolean, desc: 'Set true to force fetch page that is not within freshness criteria. Default: false'
      option :parsers, :aliases => :pw, type: :numeric, desc: 'Set how many parser workers to use. Default: 1'
      option :fetchers, :aliases => :fw, type: :numeric, desc: 'Set how many fetcher workers to use. Default: 1'
      option :browsers, :aliases => :bw, type: :numeric, desc: 'Set how many browser workers to use. Default: 0'
      option :disable_scheduler, type: :boolean, desc: 'Set true to disable scheduler. Default: false'
      option :cancel_current_job, type: :boolean, desc: 'Set true to cancel currently active job if scheduler starts. Default: false'
      option :schedule, type: :string, desc: 'Set the schedule of the scraper to run. Must be in CRON format.'
      option :timezone, type: :string, desc: "Set the scheduler's timezone. Must be in IANA Timezone format. Defaults to \"America/Toronto\""
      option :profile, type: :string, desc: 'Set the profiles (comma separated) to apply to the job. Default: default'
      option :multiple_jobs, type: :boolean, desc: 'Set true to enable multiple jobs. Default: false'
      option :max_job_count, type: :numeric, desc: 'Set a value to set max number of jobs available. Set -1 for unlimited. Default: 3'
      option :max_page_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      option :enable_global_cache, type: :boolean, desc: 'Set true to enable page cache. Default: false'
      option :retry_interval, type: :numeric, desc: 'Set a value to set retry time interval on seconds when refetching a page. Set a value grather than 0 to set it as new time to refetch, 0 means default time. Default: 0'
      option :soft_fetching_try_limit, type: :numeric, desc: 'Set the soft fetching try limit value.'
      option :soft_refetch_limit, type: :numeric, desc: 'Set the soft refetch limit value.'
      option :parsing_try_limit, type: :numeric, desc: 'Set the parsing try limit value.'
      option :prevent_kb_autoscaler, type: :boolean, desc: 'Set true to prevent the autoscaler from restarting the job. Default: false'
      def create(scraper_name, git_repository)
        # puts "options #{options}"
        client = Client::Scraper.new(options)
        puts "#{client.create(scraper_name, git_repository, options)}"
      end

      desc "update <scraper_name>", "Update a scraper"
      long_desc <<-LONGDESC
          Updates a scraper\x5
          LONGDESC
      option :branch, :aliases => :b, desc: 'Set the Git branch to use. Default: master'
      option :name, :aliases => :n, desc: 'Set the scraper name. Name can only consist of alphabets, numbers, underscores and dashes. Name must be unique to your account'
      option :repo, :aliases => :r, desc: 'Set the URL to a valid Git repository'
      option :freshness_type, :aliases => :t, desc: 'Set how fresh the page cache is. Possible values: day, week, month, year. Default: any'
      option :proxy_type, desc: 'Set the Proxy type. Default: standard'
      option :force_fetch, :aliases => :f, type: :boolean, desc: 'Set true to force fetch page that is not within freshness criteria. Default: false'
      option :parsers, :aliases => :pw, type: :numeric, desc: 'Set how many parser workers to use. Default: 1'
      option :fetchers, :aliases => :fw, type: :numeric, desc: 'Set how many fetcher workers to use. Default: 1'
      option :browsers, :aliases => :bw, type: :numeric, desc: 'Set how many browser workers to use. Default: 0'
      option :disable_scheduler, type: :boolean, desc: 'Set true to disable scheduler. Default: false'
      option :cancel_current_job, type: :boolean, desc: 'Set true to cancel currently active job if scheduler starts. Default: false'
      option :schedule, type: :string, desc: 'Set the schedule of the scraper to run. Must be in CRON format.'
      option :timezone, type: :string, desc: "Set the scheduler's timezone. Must be in IANA Timezone format. Defaults to \"America/Toronto\""
      option :profile, type: :string, desc: 'Set the profiles (comma separated) to apply to the job. Default: default'
      option :multiple_jobs, type: :boolean, desc: 'Set true to enable multiple jobs. Default: false'
      option :max_job_count, type: :numeric, desc: 'Set a value to set max number of jobs available. Set -1 for unlimited. Default: 3'
      option :max_page_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      option :enable_global_cache, type: :boolean, desc: 'Set true to enable page cache. Default: false'
      option :retry_interval, type: :numeric, desc: 'Set a value to set retry time interval on seconds when refetching a page. Set a value grather than 0 to set it as new time to refetch, 0 means default time. Default: 0'
      option :soft_fetching_try_limit, type: :numeric, desc: 'Set the soft fetching try limit value.'
      option :soft_refetch_limit, type: :numeric, desc: 'Set the soft refetch limit value.'
      option :parsing_try_limit, type: :numeric, desc: 'Set the parsing try limit value.'
      option :prevent_kb_autoscaler, type: :boolean, desc: 'Set true to prevent the autoscaler from restarting the job. Default: false'
      def update(scraper_name)
        client = Client::Scraper.new(options)
        puts "#{client.update(scraper_name, options)}"
      end


      desc "show <scraper_name>", "Show a scraper"
      def show(scraper_name)
        client = Client::Scraper.new(options)
        puts "#{client.find(scraper_name)}"
      end

      desc "delete <scraper_name>", "Delete a scraper and related records"
      def delete(scraper_name)
        client = Client::Scraper.new(options)
        puts "#{client.delete(scraper_name)}"
      end


      desc "deploy <scraper_name>", "Deploy a scraper"
      long_desc <<-LONGDESC
          Deploys a scraper
          LONGDESC
      def deploy(scraper_name)
        client = Client::ScraperDeployment.new()
        puts "Deploying scraper. This may take a while..."
        puts "#{client.deploy(scraper_name)}"
      end

      desc "start <scraper_name>", "Creates a scraping job and runs it"
      long_desc <<-LONGDESC
          Starts a scraper by creating an active scrape job\x5
          LONGDESC
      option :parsers, :aliases => :pw, type: :numeric, desc: 'Set how many parser workers to use. Default: 1'
      option :fetchers, :aliases => :fw, type: :numeric, desc: 'Set how many fetcher workers to use. Default: 1'
      option :browsers, :aliases => :bw, type: :numeric, desc: 'Set how many browser workers to use. Default: 0'
      option :proxy_type, desc: 'Set the Proxy type. Default: standard'
      option :vars, type: :string, banner: :JSON, desc: 'Set input vars. Must be in json format. i.e: [{"name":"foo", "value":"bar", "secret":false}] '
      option :max_page_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      option :retry_interval, type: :numeric, desc: 'Set a value to set retry time interval on seconds when refetching a page. Set a value grather than 0 to set it as new time to refetch, 0 means default time. Default: 0'
      option :soft_fetching_try_limit, type: :numeric, desc: 'Set the soft fetching try limit value.'
      option :soft_refetch_limit, type: :numeric, desc: 'Set the soft refetch limit value.'
      option :parsing_try_limit, type: :numeric, desc: 'Set the parsing try limit value.'
      option :prevent_kb_autoscaler, type: :boolean, desc: 'Set true to prevent the autoscaler from restarting the job. Default: false'
      def start(scraper_name)
        client = Client::ScraperJob.new(options)
        puts "Starting a scrape job..."
        puts "#{client.create(scraper_name, options)}"
      end


      desc "log <scraper_name>", "List log entries related to a scraper's current job"
      long_desc <<-LONGDESC
          Shows log related to a scraper's current job. Defaults to showing the most recent entries\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :head, :aliases => :H, desc: 'Show the oldest log entries. If not set, newest entries is shown'
      option :parsing, :aliases => :p, type: :boolean, desc: 'Show only log entries related to parsing errors'
      option :seeding, :aliases => :s, type: :boolean, desc: 'Show only log entries related to seeding errors'
      option :finisher, :aliases => :f, type: :boolean, desc: 'Show only log entries related to finisher errors'
      option :more, :aliases => :m, desc: 'Show next set of log entries. Enter the `More token`'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 5000 per page.'
      def log(scraper_name)
        client = Client::JobLog.new(options)

        query = {}
        query["order"] = options.delete(:head) if options[:head]
        query["job_type"] = "parsing" if options[:parsing]
        query["job_type"] = "seeding" if options[:seeding]
        query["job_type"] = "finisher executing" if options[:finisher]
        query["page_token"] = options.delete(:more) if options[:more]
        query["per_page"] = options.delete(:per_page) if options[:per_page]

        puts "Fetching logs..."

        if options[:job]
          result = client.all_job_log(options[:job], {query: query})
        else
          result = client.scraper_all_job_log(scraper_name, {query: query})
        end

        if result['entries'].nil? || result["entries"].length == 0
          puts "No logs yet, please try again later."
        else
          more_token = result["more_token"]

          result["entries"].each do |entry|
            puts "#{entry["timestamp"]} #{entry["severity"]}: #{entry["payload"]}" if entry.is_a?(Hash)
          end

          unless more_token.nil?
            puts "-----------"
            puts "To see more entries, add: \"--more #{more_token}\""
          end
        end
      end

      desc "stats <scraper_name>", "Get the stat for a current job (Defaults to showing data from cached stats)"
      long_desc <<-LONGDESC
        Get stats for a scraper's current job\n
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :live, type: :boolean, desc: 'Get data from the live stats, not cached stats.'
      def stats(scraper_name)
        client = Client::JobStat.new(options)
        if options[:job]
          puts "#{client.job_current_stats(options[:job], options)}"
        else
          puts "#{client.scraper_job_current_stats(scraper_name, options)}"
        end
      end

      desc "history <scraper_name>", "Get historic stats for a job"
      long_desc <<-LONGDESC
        Get historic stats for a scraper's current job\n
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :"min-timestamp", type: :string, desc: 'Starting timestamp point in time to query historic stats (inclusive)'
      option :"max-timestamp", type: :string, desc: 'Ending timestamp point in time to query historic stats (inclusive)'
      option :"limit", type: :numeric, desc: 'Limit stats retrieved'
      option :"order", type: :numeric, desc: 'Order stats by timestamp [DESC]'
      option :live, type: :boolean, desc: 'Get data from the live stats history, not cached stats history.'
      option :filter, type: :string, desc: 'Filter results on `day` or `hour`, if not specified will return all records.'
      def history(scraper_name)
        client = Client::JobStat.new(options)
        if options[:job]
          json = JSON.parse(client.job_stats_history(options[:job], options).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        else
          json = JSON.parse(client.scraper_job_stats_history(scraper_name, options).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        end
      end

      desc "profile <scraper_name>", "displays the scraper applied profile"
      long_desc <<-LONGDESC
        Displays the account applied profile
      LONGDESC
      def profile(scraper_name)
        client = Client::Scraper.new(options)
        puts "#{client.profile(scraper_name)}"
      end

      desc "job SUBCOMMAND ...ARGS", "manage scrapers jobs"
      subcommand "job", ScraperJob

      desc "deployment SUBCOMMAND ...ARGS", "manage scrapers deployments"
      subcommand "deployment", ScraperDeployment

      desc "finisher SUBCOMMAND ...ARGS", "manage scrapers finishers"
      subcommand "finisher", ScraperFinisher

      desc "output SUBCOMMAND ...ARGS", "view scraper outputs"
      subcommand "output", JobOutput

      desc "page SUBCOMMAND ...ARGS", "manage pages on a job"
      subcommand "page", ScraperPage

      desc "export SUBCOMMAND ...ARGS", "manage scraper's exports"
      subcommand "export", ScraperExport

      desc "exporter SUBCOMMAND ...ARGS", "manage scraper's exporters"
      subcommand "exporter", ScraperExporter

      desc "var SUBCOMMAND ...ARGS", "for managing scraper's variables"
      subcommand "var", ScraperVar

      desc "task SUBCOMMAND ...ARGS", "manage task on a job"
      subcommand "task", ScraperTask

      desc "resource SUBCOMMAND ...ARGS", "manage resource on a job"
      subcommand "resource", ScraperResource


    end
  end

end
