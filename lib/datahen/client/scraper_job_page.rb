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

      def enqueue(scraper_name, method, url, opts={})
        body = {}
        body[:method] =  method != "" ? method : "GET"
        body[:url] =  url
        body[:page_type] = opts[:page_type] if opts[:page_type]
        body[:priority] = opts[:priority] if opts[:priority]
        body[:fetch_type] = opts[:fetch_type] if opts[:fetch_type]
        body[:body] = opts[:body] if opts[:body]
        body[:headers] = opts[:headers] if opts[:headers]
        body[:vars] = opts[:vars] if opts[:vars]
        body[:force_fetch] = opts[:force_fetch] if opts[:force_fetch]
        body[:freshness] = opts[:freshness] if opts[:freshness]
        body[:ua_type] = opts[:ua_type] if opts[:ua_type]
        body[:no_redirect] = opts[:no_redirect] if opts[:no_redirect]
        body[:cookie] = opts[:cookie] if opts[:cookie]
        body[:max_size] = opts[:max_size] if opts[:max_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)

        params = @options.merge({body: body.to_json})

        self.class.post("/scrapers/#{scraper_name}/current_job/pages", params)
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
