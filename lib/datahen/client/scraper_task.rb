module Datahen
  module Client
    class ScraperTask < Datahen::Client::Base
      def all(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/current_job/tasks", params)
      end

      def find(scraper_name, task_id, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/current_job/tasks/#{task_id}", params)
      end

    end

  end
end
