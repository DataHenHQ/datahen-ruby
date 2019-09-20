module Datahen
  module Client
    class ScraperDeployment < Datahen::Client::Base

      def all(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/deployments", params)
      end


      def deploy(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.post("/scrapers/#{scraper_name}/deployments", params)
      end

    end
  end
end
