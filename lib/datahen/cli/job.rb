module Datahen
  class CLI < Thor
    class Job < Thor
      package_name "job"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "list", "gets a list of jobs"
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :status, type: :string, desc: 'Returns jobs with a certain status'
      long_desc <<-LONGDESC
        List scrape jobs.
      LONGDESC
      def list()
        client = Client::Job.new(options)
        puts "#{client.all()}"
      end

      desc "show <job_id>", "Show a job (Defaults to showing data from cached job)"
      option :live, type: :boolean, desc: 'Get data from the live job, not cached job.'
      def show(job_id)
        client = Client::Job.new(options)
        puts "#{client.find(job_id, options)}"
      end

      desc "stats <job_id>", "Get the stat for a job (Defaults to showing data from cached stats)"
      long_desc <<-LONGDESC
        Get stats for a scraper's current job\n
      LONGDESC
      option :live, type: :boolean, desc: 'Get data from the live stats, not cached stats.'
      def stats(job_id)
        client = Client::JobStat.new(options)
        puts "#{client.job_current_stats(job_id, options)}"
      end
      
    end
  end

end
