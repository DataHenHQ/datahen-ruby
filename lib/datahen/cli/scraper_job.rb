module Datahen
  class CLI < Thor
    class ScraperJob < Thor
      package_name "scraper job"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "show <scraper_name>", "Show a scraper's current job (Defaults to showing data from cached job)"
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :live, type: :boolean, desc: 'Get data from the live job, not cached job.'
      def show(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.find(options[:job], options)}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.find(scraper_name, options)}"
        end
      end


      desc "list <scraper_name>", "gets a list of jobs on a scraper"
      long_desc <<-LONGDESC
        List jobs on a scraper.
      LONGDESC
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :status, type: :string, desc: 'Returns jobs with a certain status'
      def list(scraper_name)
        client = Client::ScraperJob.new(options)
        puts "#{client.all(scraper_name)}"
      end


      desc "cancel <scraper_name>", "cancels a scraper's current job"
      long_desc <<-LONGDESC
        Cancels a scraper's current job
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def cancel(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.cancel(options[:job])}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.cancel(scraper_name)}"
        end
      end

      desc "delete <scraper_name>", "delete a scraper's current job"
      long_desc <<-LONGDESC
        Delete a scraper's current job
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def delete(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.delete(options[:job])}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.delete(scraper_name)}"
        end
      end

      desc "resume <scraper_name>", "resumes a scraper's current job"
      long_desc <<-LONGDESC
        Resumes a scraper's current job
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def resume(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.resume(options[:job])}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.resume(scraper_name)}"
        end
      end

      desc "pause <scraper_name>", "pauses a scraper's current job"
      long_desc <<-LONGDESC
        Pauses a scraper's current job
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :force, :aliases => :f, type: :boolean, desc: 'Force a job to be paused from a done or cancelled status'
      def pause(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.pause(options[:job], options)}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.pause(scraper_name, options)}"
        end
      end


      desc "update <scraper_name>", "updates a scraper's current job"
      long_desc <<-LONGDESC
        Updates a scraper's current job.
      LONGDESC
      option :workers, :aliases => :w, type: :numeric, desc: 'Set how many standard workers to use. Scraper job must be restarted(paused then resumed, or cancelled then resumed) for it to take effect. Default: 1. '
      option :browsers, type: :numeric, desc: 'Set how many browser workers to use. Scraper job must be restarted(paused then resumed, or cancelled then resumed) for it to take effect. Default: 0. '
      option :proxy_type, desc: 'Set the Proxy type. Default: standard'
      option :profile, type: :string, desc: 'Set the profiles (comma separated) to apply to the job. Default: default'
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :max_page_size, type: :numeric, desc: 'Set a value to set max page size when fetching a page. Set a value grather than 0 to set it as limit, 0 means any size. Default: 0'
      def update(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.update(options[:job], options)}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.update(scraper_name, options)}"
        end
      end

      desc "profile <scraper_name>", "displays a scraper's current job applied profile"
      long_desc <<-LONGDESC
        Displays a scraper's current job applied profile
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def profile(scraper_name)
        if options[:job]
          client = Client::Job.new(options)
          puts "#{client.profile(options[:job])}"
        else
          client = Client::ScraperJob.new(options)
          puts "#{client.profile(scraper_name)}"
        end
      end

      desc "var SUBCOMMAND ...ARGS", "for managing scraper's job variables"
      subcommand "var", ScraperJobVar


    end
  end

end
