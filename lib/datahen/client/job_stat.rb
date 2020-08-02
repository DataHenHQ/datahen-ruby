module Datahen
  module Client
    class JobStat < Datahen::Client::Base

      def job_current_stats(job_id, opts={})
        if opts[:live]
          self.class.get("/jobs/#{job_id}/stats/current", @options)
        else
          self.class.get("/cached/jobs/#{job_id}/stats/current", @options)
        end
      end

      def scraper_job_current_stats(scraper_name, opts={})
        if opts[:live]
          self.class.get("/scrapers/#{scraper_name}/current_job/stats/current", @options)
        else
          self.class.get("/cached/scrapers/#{scraper_name}/current_job/stats/current", @options)
        end
      end

      def job_stats_history(job_id, opts={})
        if opts[:live]
          self.class.get("/jobs/#{job_id}/stats/history", @options)
        else
          self.class.get("/cached/jobs/#{job_id}/stats/history", @options)
        end
      end

      def scraper_job_stats_history(scraper_name, opts={})
        if opts[:live]
          self.class.get("/scrapers/#{scraper_name}/current_job/stats/history", @options)
        else
          self.class.get("/cached/scrapers/#{scraper_name}/current_job/stats/history", @options)
        end
      end

    end
  end
end
