module Datahen
  class CLI < Thor
    class ScraperPage < Thor

      package_name "scraper page"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "list <scraper_name>", "List Pages on a scraper's current job"
      long_desc <<-LONGDESC
        List all pages in a scraper's current job.\x5
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :page_type, :aliases => :t, type: :string, desc: 'Filter by page_type'
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :fetch_fail, type: :boolean, desc: 'Returns only pages that fails fetching.'
      option :parse_fail, type: :boolean, desc: 'Returns only pages that fails parsing.'
      option :status, type: :string, desc: 'Returns only pages with specific status.'
      def list(scraper_name)
        if options[:job]
          client = Client::JobPage.new(options)
          puts "#{client.all(options[:job])}"
        else
          client = Client::ScraperJobPage.new(options)
          puts "#{client.all(scraper_name)}"
        end
      end

      desc "add <scraper_name> <url>", "Enqueues a page to a scraper's current job"
      long_desc <<-LONGDESC
          Enqueues a page to a scraper's current job\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :method, :aliases => :m, type: :string, desc: 'Set request method. Default: GET'
      option :headers, :aliases => :H, type: :string, banner: :JSON, desc: 'Set request headers. Must be in json format. i.e: {"Foo":"bar"} '
      option :cookie, :aliases => :c, type: :string, desc: 'Set request cookie.'
      option :vars, :aliases => :v, type: :string, banner: :JSON, desc: 'Set user-defined page variables. Must be in json format. i.e: {"Foo":"bar"}'
      option :page_type, :aliases => :t, desc: 'Set page type'
      option :priority, type: :numeric, desc: 'Set fetch priority. The higher the value, the sooner the page gets fetched. Default: 0'
      option :fetch_type, :aliases => :F, desc: 'Set fetch type. Default: http'
      option :body, :aliases => :b, desc: 'Set request body'
      option :force_fetch, :aliases => :f, type: :boolean, desc: 'Set true to force fetch page that is not within freshness criteria. Default: false'
      option :freshness, :aliases => :s, desc: 'Set how fresh the page cache is. Accepts timestap format.'
      option :ua_type, :aliases => :u, desc: 'Set user agent type. Default: desktop'
      option :no_redirect, :aliases => :n, type: :boolean, desc: 'Set true to not follow redirect. Default: false'
      option :max_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      def add(scraper_name, url)
        begin
          options[:headers] = JSON.parse(options[:headers]) if options[:headers]
          options[:vars] = JSON.parse(options[:vars]) if options[:vars]
          method = options[:method]

          if options[:job]
            client = Client::JobPage.new(options)
            puts "#{client.enqueue(options[:job], method, url, options)}"
          else
            client = Client::ScraperJobPage.new(options)
            puts "#{client.enqueue(scraper_name, method, url, options)}"
          end

        rescue JSON::ParserError
          if options[:headers]
            puts "Error: #{options[:headers]} on headers is not a valid JSON"
          end
          if options[:vars]
            puts "Error: #{options[:vars]} on vars is not a valid JSON"
          end
        end
      end


      desc "update <scraper_name> <gid>", "Update a page in a scraper's current job"
      long_desc <<-LONGDESC
          Updates a page in a scraper's current job. Only page_type or page vars is updateable.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :page_type, :aliases => :t, desc: 'Set page type'
      option :priority, type: :numeric, desc: 'Set fetch priority. The higher the value, the sooner the page gets fetched. Default: 0'
      option :vars, :aliases => :v, type: :string, desc: 'Set user-defined page variables. Must be in json format. i.e: {"Foo":"bar"}'
      option :max_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      def update(scraper_name, gid)
        begin
          options[:vars] = JSON.parse(options[:vars]) if options[:vars]

          if options[:job]
            client = Client::JobPage.new(options)
            puts "#{client.update(options[:job], gid, options)}"
          else
            client = Client::ScraperJobPage.new(options)
            puts "#{client.update(scraper_name, gid, options)}"
          end

        rescue JSON::ParserError
          if options[:vars]
            puts "Error: #{options[:vars]} on vars is not a valid JSON"
          end
        end
      end

      desc "refetch <scraper_name>", "Refetch Pages on a scraper's current job"
      long_desc <<-LONGDESC
        Refetch pages in a scraper's current job. You need to specify either a --gid or --fetch-fail or --parse-fail or --status or --page-type.\x5
      LONGDESC
      option :gid, :aliases => :g, type: :string, desc: 'Refetch a specific GID'
      option :fetch_fail, type: :boolean, desc: 'Refetches only pages that fails fetching.'
      option :parse_fail, type: :boolean, desc: 'Refetches only pages that fails parsing.'
      option :status, type: :string, desc: 'Refetches only pages with a specific status.'
      option :page_type, type: :string, desc: 'Refetches only pages with a specific page type.'
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def refetch(scraper_name)
        if !options.key?(:gid) && !options.key?(:fetch_fail) && !options.key?(:parse_fail) && !options.key?(:status) && !options.key?(:page_type)
          puts "Must specify either a --gid, --fetch-fail, --parse-fail, --status or --page-type"
          return
        end

        if options[:job]
          client = Client::JobPage.new(options)
          puts "#{client.refetch(options[:job])}"
        else
          client = Client::ScraperJobPage.new(options)
          puts "#{client.refetch(scraper_name)}"
        end
      end

      desc "reparse <scraper_name>", "Reparse Pages on a scraper's current job"
      long_desc <<-LONGDESC
        Reparse pages in a scraper's current job. You need to specify either a --gid or --parse-fail or --status or --page-type.\x5
      LONGDESC
      option :gid, :aliases => :g, type: :string, desc: 'Reparse a specific GID'
      option :parse_fail, type: :boolean, desc: 'Reparse only pages that fails parsing.'
      option :status, type: :string, desc: 'Reparse only pages with a specific status.'
      option :page_type, type: :string, desc: 'Refetches only pages with a specific page type.'
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def reparse(scraper_name)
        if !options.key?(:gid) && !options.key?(:parse_fail) && !options.key?(:status) && !options.key?(:page_type)
          puts "Must specify either a --gid, --parse-fail, --status or --page-type"
          return
        end

        if options[:job]
          client = Client::JobPage.new(options)
          puts "#{client.reparse(options[:job])}"
        else
          client = Client::ScraperJobPage.new(options)
          puts "#{client.reparse(scraper_name)}"
        end
      end

      desc "limbo <scraper_name>", "Move pages on a scraper's current job to limbo"
      long_desc <<-LONGDESC
        Move pages in a scraper's current job to limbo. You need to specify either a --gid or --status.\x5
      LONGDESC
      option :gid, :aliases => :g, type: :string, desc: 'Move a specific GID to limbo'
      option :status, type: :string, desc: 'Move pages with a specific status to limbo.'
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def limbo(scraper_name)
        if !options.key?(:gid) && !options.key?(:status)
          puts "Must specify either a --gid or --status"
          return
        end

        if options[:job]
          client = Client::JobPage.new(options)
          puts "#{client.limbo(options[:job])}"
        else
          client = Client::ScraperJobPage.new(options)
          puts "#{client.limbo(scraper_name)}"
        end
      end

      desc "show <scraper_name> <gid>", "Show a page in scraper's current job"
      long_desc <<-LONGDESC
          Shows a page in a scraper's current job.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def show(scraper_name, gid)
        if options[:job]
          client = Client::JobPage.new(options)
          puts "#{client.find(options[:job], gid)}"
        else
          client = Client::ScraperJobPage.new(options)
          puts "#{client.find(scraper_name, gid)}"
        end
      end

      desc "log <scraper_name> <gid>", "List log entries related to a job page"
      long_desc <<-LONGDESC
          Shows log related to a page in the job. Defaults to showing the most recent entries\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :head, :aliases => :H, desc: 'Show the oldest log entries. If not set, newest entries is shown'
      option :parsing, :aliases => :p, type: :boolean, desc: 'Show only log entries related to parsing'
      option :more, :aliases => :m, desc: 'Show next set of log entries. Enter the `More token`'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 5000 per page.'
      def log(scraper_name, gid)
        client = Client::JobLog.new(options)

        query = {}
        query["order"] = options.delete(:head) if options[:head]
        query["job_type"] = "parsing" if options[:parsing]

        query["page_token"] = options.delete(:more) if options[:more]
        query["per_page"] = options.delete(:per_page) if options[:per_page]

        puts "Fetching page logs..."

        if options[:job]
          result = client.all_job_page_log(options[:job], gid, {query: query})
        else
          result = client.scraper_all_job_page_log(scraper_name, gid, {query: query})
        end

        if result['entries'].nil? || result["entries"].length == 0
          puts "No logs yet, please try again later."
        else

          more_token = result["more_token"]

          result["entries"].each do |entry|
            puts "#{entry["timestamp"]} #{entry["severity"]}: #{entry["payload"]}" if entry.is_a?(Hash)
          end

          unless more_token.nil?
            puts "to see more entries, add: \"--more #{more_token}\""
          end
        end
      end

      desc "content <scraper_name> <gid>", "Show a page's content in scraper's current job"
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def content(scraper_name, gid)
        result = nil
        if options[:job]
          client = Client::JobPage.new(options)
          result = JSON.parse(client.find_content(options[:job], gid).to_s)
        else
          client = Client::ScraperJobPage.new(options)
          result = JSON.parse(client.find_content(scraper_name, gid).to_s)
        end

        if result['available'] == true
          puts "Preview content url: \"#{result['preview_url']}\""
          begin
            `open "#{result['preview_url']}"`
          rescue
          end
        else
          puts "Content does not exist"
        end
      end

      desc "failedcontent <scraper_name> <gid>", "Show a page's failed content in scraper's current job"
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def failedcontent(scraper_name, gid)
        result = nil
        if options[:job]
          client = Client::JobPage.new(options)
          result = JSON.parse(client.find_failed_content(options[:job], gid).to_s)
        else
          client = Client::ScraperJobPage.new(options)
          result = JSON.parse(client.find_failed_content(scraper_name, gid).to_s)
        end

        if result['available'] == true
          puts "Preview failed content url: \"#{result['preview_url']}\""
          begin
            `open "#{result['preview_url']}"`
          rescue
          end
        else
          puts "Failed Content does not exist"
        end
      end

    end
  end

end
