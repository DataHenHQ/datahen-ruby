module Datahen
  class CLI < Thor
    class Seeder < Thor
      desc "try <scraper_name> <seeder_file>", "Tries a seeder file"
      long_desc <<-LONGDESC
            Takes a seeder script and tries to execute it without saving anything.\x5
            <seeder_file>: Seeder script file will be executed.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :"keep-outputs", :aliases => :ko, type: :boolean, default: false, desc: "Don't delete existing outputs"
      def try_seed(scraper_name, seeder_file)
        if options[:job]
          job_id = options[:job]
        else
          job = Client::ScraperJob.new(options).find(scraper_name)
          job_id = job['id']
        end

        puts Datahen::Scraper::Seeder.exec_seeder(seeder_file, job_id, false, options[:"keep-outputs"])
      end

      desc "exec <scraper_name> <seeder_file>", "Executes a seeder script onto a scraper's current job."
      long_desc <<-LONGDESC
            Takes a seeder script and execute it against a job and enqueues the pages into the scraper's current job\x5
            <seeder_file>: Seeder script file that will be executed on the scraper's current job.\x5
          LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :"keep-outputs", :aliases => :ko, type: :boolean, default: false, desc: "Don't delete existing outputs"
      def exec_parse(scraper_name, seeder_file)
        if options[:job]
          job_id = options[:job]
        else
          job = Client::ScraperJob.new(options).find(scraper_name)
          job_id = job['id']
        end

        puts Datahen::Scraper::Seeder.exec_seeder(seeder_file, job_id, true)
      end
    end
  end

end
