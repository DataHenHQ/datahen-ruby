module Datahen
  module Client
    class JobPage < Datahen::Client::Base
      def find(job_id, gid)
        self.class.get("/jobs/#{job_id}/pages/#{gid}", @options)
      end

      def all(job_id, opts={})
        params = @options.merge(opts)
        self.class.get("/jobs/#{job_id}/pages", params)
      end

      def update(job_id, gid, opts={})
        body = {}
        body[:page_type] = opts[:page_type] if opts[:page_type]
        body[:priority] = opts[:priority] if opts[:priority]
        body[:vars] = opts[:vars] if opts[:vars]
        body[:max_size] = opts[:max_size] if opts[:max_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]

        params = @options.merge({body: body.to_json})

        self.class.put("/jobs/#{job_id}/pages/#{gid}", params)
      end

      def enqueue(job_id, method, url, opts={})
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
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]

        params = @options.merge({body: body.to_json})

        self.class.post("/jobs/#{job_id}/pages", params)
      end

      def dequeue(job_id, limit, page_types, parse_fetching_failed, opts = {})
        body = {
          limit: limit,
          page_types: page_types,
          parse_fetching_failed: parse_fetching_failed
        }
        params = @options.merge(opts).merge({body: body.to_json})
        self.class.put("/jobs/#{job_id}/pages/parse_dequeue", params)
      end

      def parsing_update(job_id, gid, opts={})
        body = {}
        body[:outputs] = opts.fetch(:outputs) {[]}
        body[:pages] = opts.fetch(:pages) {[]}
        body[:parsing_status] = opts.fetch(:parsing_status){ nil }
        body[:log_error] = opts[:log_error] if opts[:log_error]
        body[:keep_outputs] = !!opts[:keep_outputs] if opts.has_key?(:keep_outputs)

        params = @options.merge({body: body.to_json})

        limit = opts.has_key?(:retry_limit) ? opts.fetch(:retry_limit) : self.default_retry_limit[:parser]
        self.retry(limit, 5, "Error while updating the parser.") do
          self.class.put("/jobs/#{job_id}/pages/#{gid}/parsing_update", params)
        end
      end

      def find_content(job_id, gid)
        self.class.get("/jobs/#{job_id}/pages/#{gid}/content", @options)
      end

      def find_failed_content(job_id, gid)
        self.class.get("/jobs/#{job_id}/pages/#{gid}/failed_content", @options)
      end

      def reparse(job_id, opts={})
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/pages/reparse", params)
      end

      def refetch(job_id, opts={})
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/pages/refetch", params)
      end

      def limbo(job_id, opts={})
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/pages/limbo", params)
      end
    end
  end
end
