module Datahen
  class CLI < Thor
    class ScraperFinisher < Thor

      package_name "scraper finisher"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "reset <scraper_name>", "Reset finisher on a scraper's current job"
      long_desc <<-LONGDESC
        Reset finisher on a scraper's current job.\x5
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      def reset(scraper_name)
        if options[:job]
          client = Client::JobFinisher.new(options)
          puts "#{client.reset(options[:job])}"
        else
          client = Client::ScraperFinisher.new(options)
          puts "#{client.reset(scraper_name)}"
        end
      end
    end
  end
end
