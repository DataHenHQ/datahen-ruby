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
        body[:soft_fetching_try_limit] = opts[:soft_fetching_try_limit] if opts[:soft_fetching_try_limit]
        body[:soft_refetch_limit] = opts[:soft_refetch_limit] if opts[:soft_refetch_limit]
        body[:parsing_try_limit] = opts[:parsing_try_limit] if opts[:parsing_try_limit]

        params = @options.merge({body: body.to_json})

        self.class.put("/jobs/#{job_id}/pages/#{gid}", params)
      end

      def enqueue(job_id, page, opts={})
        params = @options.merge(opts).merge({body: page.to_json})

        self.class.post("/jobs/#{job_id}/pages", params)
        
      end

      def get_gid(job_id, page, opts={})
      
        params = @options.merge(opts).merge({body: page.to_json})

        self.class.post("/jobs/#{job_id}/generate_gid", params)
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
        body[:parsing_try_limit] = opts[:parsing_try_limit] if opts.fetch(:parsing_try_limit){ nil }

        params = @options.merge({body: body.to_json})

        limit = opts.has_key?(:retry_limit) ? opts.fetch(:retry_limit) : self.default_retry_limit[:parser]
        self.retry(limit, 5, "Error while updating the parser.") do
          response = self.class.put("/jobs/#{job_id}/pages/#{gid}/parsing_update", params)
          if response.code == 422 && response.body.to_s =~ /pq:\s*deadlock/i
            raise Error::CustomRetryError.new(self.class.random_delay(5), response.body.to_s)
          end
          response
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

      def still_alive(job_id, gid, opts)
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/pages/#{gid}/still_alive", params)
      end
    end
  end
end
