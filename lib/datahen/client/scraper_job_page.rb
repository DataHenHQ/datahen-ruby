module Datahen
  module Client
    class ScraperJobPage < Datahen::Client::Base
      def find(scraper_name, gid)
        self.class.get("/scrapers/#{scraper_name}/current_job/pages/#{gid}", @options)
      end

      def all(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/#{scraper_name}/current_job/pages", params)
      end

      def update(scraper_name, gid, opts={})
        body = {}
        body[:page_type] = opts[:page_type] if opts[:page_type]
        body[:priority] = opts[:priority] if opts[:priority]
        body[:vars] = opts[:vars] if opts[:vars]
        body[:max_size] = opts[:max_size] if opts[:max_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]

        params = @options.merge({body: body.to_json})

        self.class.put("/scrapers/#{scraper_name}/current_job/pages/#{gid}", params)
      end

      def refetch(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.put("/scrapers/#{scraper_name}/current_job/pages/refetch", params)
      end

      # Deprecated, please use Datahen::Client::JobPage#refetch instead.
      #
      # @note This method will be removed at some point in the future.
      def refetch_by_job(job_id, opts={})
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/pages/refetch", params)
      end

      def reparse(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.put("/scrapers/#{scraper_name}/current_job/pages/reparse", params)
      end

      def limbo(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.put("/scrapers/#{scraper_name}/current_job/pages/limbo", params)
      end

      def enqueue(scraper_name, page, opts={})
      params = @options.merge(opts).merge({body: page.to_json})

        self.class.post("/scrapers/#{scraper_name}/current_job/pages", params)
      end

      def get_gid(scraper_name, page, opts={})
      
        params = @options.merge(opts).merge({body: page.to_json})

        self.class.post("/scrapers/#{scraper_name}/current_job/generate_gid", params)
      end

      def find_content(scraper_name, gid)
        self.class.get("/scrapers/#{scraper_name}/current_job/pages/#{gid}/content", @options)
      end

      def find_failed_content(scraper_name, gid)
        self.class.get("/scrapers/#{scraper_name}/current_job/pages/#{gid}/failed_content", @options)
      end

    end
  end
end
