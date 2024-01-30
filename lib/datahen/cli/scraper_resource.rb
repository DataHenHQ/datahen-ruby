module Datahen
  class CLI < Thor
    class ScraperResource < Thor
      package_name "scraper resource"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "list", "List resources on a scraper's current job"
      long_desc <<-LONGDESC
        List all resources in a scraper's current job or given job ID.\x5
      LONGDESC
      option :scraper_name, :aliases => :s, type: :string, desc: 'Filter by a specific scraper_name'
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :pod, type: :string, desc: 'Returns only tasks with specific pod.'
      option :container, type: :string, desc: 'Returns only tasks with specific container.'
      option :executor, type: :string, desc: 'Returns only tasks with specific executor.'
      def list()
        if options[:job]
          client = Client::JobResource.new(options)
          puts "#{client.all(options[:job])}"
        else
          if options[:scraper_name]
            client = Client::ScraperResource.new(options)
            puts "#{client.all(options[:scraper_name])}"
          else
            puts 'Must specify either a job ID or a scraper name'
          end
        end
      end
      
    end
  end

end
