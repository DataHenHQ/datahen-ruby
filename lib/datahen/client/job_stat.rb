module Datahen
  module Client
    class JobStat < Datahen::Client::Base

      def job_current_stats(job_id)
        self.class.get("/jobs/#{job_id}/stats/current", @options)
      end

      def scraper_job_current_stats(scraper_name)
        self.class.get("/scrapers/#{scraper_name}/current_job/stats/current", @options)
      end

    end
  end
end

