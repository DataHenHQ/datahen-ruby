module Datahen
  module Client
    class ScraperResource < Datahen::Client::Base
      def all(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/resources", params)
      end
    end

  end
end
