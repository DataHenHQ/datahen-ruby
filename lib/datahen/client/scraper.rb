module Datahen
  module Client
    class Scraper < Datahen::Client::Base

      def find(scraper_name)
        self.class.get("/scrapers/#{scraper_name}", @options)
      end

      def all(opts={})
        params = @options.merge opts
        self.class.get("/scrapers", params)
      end

      def create(scraper_name, git_repository, opts={})
        body = {}
        body[:name] = scraper_name
        body[:git_repository] = git_repository
        body[:git_branch] = opts[:branch] || opts[:git_branch] || "master" if opts[:branch] || opts[:git_branch]
        body[:freshness_type] = opts[:freshness_type] if opts[:freshness_type]
        body[:force_fetch] = opts[:force_fetch] if opts[:force_fetch]
        body[:parser_worker_count] = opts[:parsers] || opts[:parser_worker_count] if opts[:parsers] || opts[:parser_worker_count]
        body[:fetcher_worker_count] = opts[:fetchers] || opts[:fetcher_worker_count] if opts[:fetchers] || opts[:fetcher_worker_count]
        body[:browser_worker_count] = opts[:browsers] || opts[:browser_worker_count] if opts[:browsers] || opts[:browser_worker_count]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:disable_scheduler] = opts[:disable_scheduler] if opts[:disable_scheduler]
        body[:cancel_current_job] = opts[:cancel_current_job] if opts[:cancel_current_job]
        body[:schedule] = opts[:schedule] if opts[:schedule]
        body[:timezone] = opts[:timezone] if opts[:timezone]
        body[:profile] = opts[:profile] if opts[:profile]
        body[:multiple_jobs] = opts[:multiple_jobs] if opts[:multiple_jobs]
        body[:max_job_count] = opts[:max_job_count] if opts[:max_job_count]
        body[:max_page_size] = opts[:max_page_size] if opts[:max_page_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]
        body[:soft_fetching_try_limit] = opts[:soft_fetching_try_limit] if opts[:soft_fetching_try_limit]
        body[:soft_refetch_limit] = opts[:soft_refetch_limit] if opts[:soft_refetch_limit]
        body[:parsing_try_limit] = opts[:parsing_try_limit] if opts[:parsing_try_limit]
        body[:prevent_kb_autoscaler] = opts[:prevent_kb_autoscaler] if opts.has_key?("prevent_kb_autoscaler") || opts.has_key?(:prevent_kb_autoscaler)
        params = @options.merge({body: body.to_json})
        self.class.post("/scrapers", params)
      end

      def update(scraper_name, opts={})
        body = {}
        body[:name] = opts[:name] if opts[:name]
        body[:git_repository] = opts[:repo] || opts[:git_repository] if opts[:repo] || opts[:git_repository]
        body[:git_branch] = opts[:branch] || opts[:git_branch] if opts[:branch] || opts[:git_branch]
        body[:freshness_type] = opts[:freshness_type] if opts[:freshness_type]
        body[:force_fetch] = opts[:force_fetch] if opts.has_key?("force_fetch") || opts.has_key?(:force_fetch)
        body[:parser_worker_count] = opts[:parsers] || opts[:parser_worker_count] if opts[:parsers] || opts[:parser_worker_count]
        body[:fetcher_worker_count] = opts[:fetchers] || opts[:fetcher_worker_count] if opts[:fetchers] || opts[:fetcher_worker_count]
        body[:browser_worker_count] = opts[:browsers] || opts[:browser_worker_count] if opts[:browsers] || opts[:browser_worker_count]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:disable_scheduler] = opts[:disable_scheduler] if opts.has_key?("disable_scheduler") || opts.has_key?(:disable_scheduler)
        body[:cancel_current_job] = opts[:cancel_current_job] if opts.has_key?("cancel_current_job") || opts.has_key?(:cancel_current_job)
        body[:schedule] = opts[:schedule] if opts[:schedule]
        body[:timezone] = opts[:timezone] if opts[:timezone]
        body[:profile] = opts[:profile] if opts[:profile]
        body[:multiple_jobs] = opts[:multiple_jobs] if opts.has_key?("multiple_jobs") || opts.has_key?(:multiple_jobs)
        body[:max_job_count] = opts[:max_job_count] if opts.has_key?("max_job_count") || opts.has_key?(:max_job_count)
        body[:max_page_size] = opts[:max_page_size] if opts.has_key?("max_page_size") || opts.has_key?(:max_page_size)
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]
        body[:soft_fetching_try_limit] = opts[:soft_fetching_try_limit] if opts[:soft_fetching_try_limit]
        body[:soft_refetch_limit] = opts[:soft_refetch_limit] if opts[:soft_refetch_limit]
        body[:parsing_try_limit] = opts[:parsing_try_limit] if opts[:parsing_try_limit]
        body[:prevent_kb_autoscaler] = opts[:prevent_kb_autoscaler] if opts.has_key?("prevent_kb_autoscaler") || opts.has_key?(:prevent_kb_autoscaler)
        params = @options.merge({body: body.to_json})

        self.class.put("/scrapers/#{scraper_name}", params)
      end

      def delete(scraper_name, opts={})
        params = @options.merge(opts)
        self.class.delete("/scrapers/#{scraper_name}", params)
      end

      def profile(scraper_name, opts={})
        params = @options.merge(opts)

        self.class.get("/scrapers/#{scraper_name}/profile", params)
      end

    end
  end
end
