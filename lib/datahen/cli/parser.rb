module Datahen
  class CLI < Thor
    class Parser < Thor
      desc "try <scraper_name> <parser_file> <GID>", "Tries a parser on a Job Page"
      long_desc <<-LONGDESC
            Takes a parser script and runs it against a job page\x5
            <parser_file>: Parser script file that will be executed on the page.\x5
            <GID>: Global ID of the page.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :global, :aliases => :g, type: :boolean, default: false, desc: 'Use globalpage instead of a job page'
      option :vars, :aliases => :v, type: :string, desc: 'Set user-defined page variables. Must be in json format. i.e: {"Foo":"bar"}'
      option :"keep-outputs", :aliases => :ko, type: :boolean, default: false, desc: "Don't delete existing outputs"
      def try_parse(scraper_name, parser_file, gid)
        begin

          if options[:job]
            job_id = options[:job]
          elsif options[:global]
            job_id = nil
          else
            job = Client::ScraperJob.new(options).find(scraper_name)
            job_id = job['id']
          end

          vars = JSON.parse(options[:vars]) if options[:vars]
          puts Datahen::Scraper::Parser.exec_parser_page(parser_file, gid, job_id, false, vars, options[:"keep-outputs"])

        rescue JSON::ParserError
          if options[:vars]
            puts "Error: #{options[:vars]} on vars is not a valid JSON"
          end
        end
      end

      desc "exec <scraper_name> <parser_file> <GID>...<GID>", "Executes a parser script on one or more Job Pages within a scraper's current job"
      long_desc <<-LONGDESC
            Takes a parser script executes it against a job page(s) and save the output to the scraper's current job\x5
            <parser_file>: Parser script file will be executed on the page.\x5
            <GID>: Global ID of the page.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :vars, :aliases => :v, type: :string, desc: 'Set user-defined page variables. Must be in json format. i.e: {"Foo":"bar"}'
      option :"keep-outputs", :aliases => :ko, type: :boolean, default: false, desc: "Don't delete existing outputs"
      def exec_parse(scraper_name, parser_file, *gids)
        if options[:job]
          job_id = options[:job]
        else
          job = Client::ScraperJob.new(options).find(scraper_name)
          job_id = job['id']
        end

        gids.each do |gid|
          begin
            puts "Parsing #{gid}"

            vars = JSON.parse(options[:vars]) if options[:vars]
            puts Datahen::Scraper::Parser.exec_parser_page(parser_file, gid, job_id, true, vars, options[:"keep-outputs"])
          rescue => e
            puts e
          end
        end
      end

      desc "batch <scraper_name> <config_file>", "Dequeue and execute Job Pages within a scraper's current job"
      long_desc <<-LONGDESC
            Dequeue pending job page(s) to execute their scripts and save the output to the scraper's current job\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :"keep-outputs", :aliases => :ko, type: :boolean, default: false, desc: "Don't delete existing outputs"
      option :"workers", type: :numeric, default: 1, desc: "Worker count"
      option :"max-garbage", type: :numeric, default: 5, desc: "Pages processed before calling the garbage collector"
      option :"dequeue-interval", type: :numeric, default: 1000, desc: "Nanoseconds to wait between dequeueing"
      option :"dequeue-scale", type: :numeric, default: 1.5, desc: "Scale vs worker count describing how many pages to dequeue"
      def batch_exec_parse(scraper_name, config_file)
        if options[:job]
          job_id = options[:job]
        else
          job = Client::ScraperJob.new(options).find(scraper_name)
          job_id = job['id']
        end

        begin
          batch = Datahen::Scraper::BatchParser.new job_id, config_file,
            worker_count: options[:"workers"],
            max_garbage: options[:"max-garbage"],
            dequeue_interval: options[:"dequeue-interval"],
            dequeue_scale: options[:"dequeue-scale"]
          batch.exec_parse true, options[:"keep-outputs"]
        rescue => e
          puts e
        end
      end
    end
  end

end
