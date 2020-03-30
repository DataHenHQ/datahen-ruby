module Datahen
  module Scraper
    class Seeder

      def self.exec_seeder(filename, job_id=nil, save=false, keep_outputs=false)
        extname = File.extname(filename)
        case extname
        when '.rb'
          executor = RubySeederExecutor.new(
            filename: filename,
            job_id: job_id,
            keep_outputs: keep_outputs
          )
          executor.exec_seeder(save)
        else
          puts "Unable to find a seeder executor for file type \"#{extname}\""
        end
      end

    end
  end
end
