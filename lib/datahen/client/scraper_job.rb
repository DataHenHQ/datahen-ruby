module Datahen
  module Client
    class ScraperJob < Datahen::Client::Base
      def all(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/jobs", params)
      end

      def create(scraper_name, opts={})
        body = {}
        body[:standard_worker_count] = opts[:workers] if opts[:workers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:max_page_size] = opts[:max_page_size] if opts[:max_page_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        if opts[:vars]
          if opts[:vars].is_a?(Array)
            body[:vars] = opts[:vars]
          elsif opts[:vars].is_a?(String)
            body[:vars] = JSON.parse(opts[:vars])
          end
        end
        params = @options.merge({body: body.to_json})
        self.class.post("/scrapers/#{scraper_name}/jobs", params)
      end

      def find(scraper_name, opts={})
        if opts[:live]
          self.class.get("/scrapers/#{scraper_name}/current_job", @options)
        else
          self.class.get("/cached/scrapers/#{scraper_name}/current_job", @options)
        end
      end

      def update(scraper_name, opts={})
        body = {}
        body[:status] = opts[:status] if opts[:status]
        body[:standard_worker_count] = opts[:workers] if opts[:workers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:profile] = opts[:profile] if opts[:profile]
        body[:max_page_size] = opts[:max_page_size] if opts[:max_page_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        params = @options.merge({body: body.to_json})

        self.class.put("/scrapers/#{scraper_name}/current_job", params)
      end

      def cancel(scraper_name, opts={})
        opts[:status] = 'cancelled'
        update(scraper_name, opts)
      end

      def resume(scraper_name, opts={})
        opts[:status] = 'active'
        update(scraper_name, opts)
      end

      def pause(scraper_name, opts={})
        opts[:status] = 'paused'
        update(scraper_name, opts)
      end

      def profile(scraper_name, opts={})
        params = @options.merge(opts)

        self.class.get("/scrapers/#{scraper_name}/current_job/profile", params)
      end

      def delete(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.delete("/scrapers/#{scraper_name}/current_job", params)
      end
    end
  end
end
