module Datahen
  class CLI < Thor
    class ScraperTask < Thor
      package_name "scraper task"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "list <scraper_name>", "List Tasks on a scraper's current job"
      long_desc <<-LONGDESC
        List all tasks in a scraper's current job or given job ID.\x5
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :status, type: :array, desc: 'Returns only tasks with specific status.'
      option :action, type: :array, desc: 'Returns only tasks with specific action.'
      option :"include-system", type: :boolean, desc: 'If it is true, will returns all actions. If it is false only tasks with specific action ["refetch", "reparse", "terminate"].'
      def list(scraper_name)
        if options[:job]
          client = Client::JobTask.new(options)
          puts "#{client.all(options[:job])}"
        else
          client = Client::ScraperTask.new(options)
          puts "#{client.all(scraper_name)}"
        end
      end


      desc "show <scraper_name> <task_id>", "Show task in scraper's current job"
      long_desc <<-LONGDESC
          Shows a task in a scraper's current job or given job ID.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def show(scraper_name, task_id)
        if options[:job]
          client = Client::JobTask.new(options)
          puts "#{client.find(options[:job], task_id)}"
        else
          client = Client::ScraperTask.new(options)
          puts "#{client.find(scraper_name, task_id)}"
        end
      end
      
    end
  end

end
