module Datahen
  module Client
    class Job < Datahen::Client::Base
      def all(opts={})
        params = @options.merge(opts)
        self.class.get("/jobs", params)
      end

      def find(job_id, opts={})
        if opts[:live]
          self.class.get("/jobs/#{job_id}", @options)
        else
          self.class.get("/cached/jobs/#{job_id}", @options)
        end
      end

      def update(job_id, opts={})
        body = {}
        body[:status] = opts[:status] if opts[:status]
        body[:parser_worker_count] = opts[:parsers] if opts[:parsers]
        body[:fetcher_worker_count] = opts[:fetchers] if opts[:fetchers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:profile] = opts[:profile] if opts[:profile]
        body[:max_page_size] = opts[:max_page_size] if opts[:max_page_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
        body[:retry_interval] = opts[:retry_interval] if opts[:retry_interval]
        body[:soft_fetching_try_limit] = opts[:soft_fetching_try_limit] if opts[:soft_fetching_try_limit]
        body[:soft_refetch_limit] = opts[:soft_refetch_limit] if opts[:soft_refetch_limit]
        body[:parsing_try_limit] = opts[:parsing_try_limit] if opts[:parsing_try_limit]
        body[:prevent_kb_autoscaler] = opts[:prevent_kb_autoscaler] if opts.has_key?("prevent_kb_autoscaler") || opts.has_key?(:prevent_kb_autoscaler)
        params = @options.merge({body: body.to_json})

        self.class.put("/jobs/#{job_id}", params)
      end

      def cancel(job_id, opts={})
        opts[:status] = 'cancelled'
        update(job_id, opts)
      end

      def resume(job_id, opts={})
        opts[:status] = 'active'
        update(job_id, opts)
      end

      def pause(job_id, opts={})
        opts[:status] = 'paused'
        update(job_id, opts)
      end

      def seeding_update(job_id, opts={})
        body = {}
        body[:outputs] = opts.fetch(:outputs) {[]}
        body[:pages] = opts.fetch(:pages) {[]}
        body[:seeding_status] = opts.fetch(:seeding_status){ nil }
        body[:log_error] = opts[:log_error] if opts[:log_error]
        body[:keep_outputs] = !!opts[:keep_outputs] if opts.has_key?(:keep_outputs)

        params = @options.merge({body: body.to_json})

        limit = opts.has_key?(:retry_limit) ? opts.fetch(:retry_limit) : self.default_retry_limit[:seeder]
        self.retry(limit, 5, "Error while updating the seeder.", false, CHECK_EMPTY_BODY) do
          response = self.class.put("/jobs/#{job_id}/seeding_update", params)
          if response.code == 422 && response.body.to_s =~ /pq:\s*deadlock/i
            raise CustomRetryError.new(self.class.random_delay(5), response.body.to_s)
          end
          response
        end
      end

      def finisher_update(job_id, opts={})
        body = {}
        body[:outputs] = opts.fetch(:outputs) {[]}
        body[:finisher_status] = opts.fetch(:finisher_status){ nil }
        body[:log_error] = opts[:log_error] if opts[:log_error]

        params = @options.merge({body: body.to_json})

        limit = opts.has_key?(:retry_limit) ? opts.fetch(:retry_limit) : self.default_retry_limit[:finisher]
        self.retry(limit, 5, "Error while updating the finisher.", false, CHECK_EMPTY_BODY) do
          response = self.class.put("/jobs/#{job_id}/finisher_update", params)
          if response.code == 422 && response.body.to_s =~ /pq:\s*deadlock/
            raise CustomRetryError.new(self.class.random_delay(5), response.body.to_s)
          end
          response
        end
      end

      def profile(job_id, opts={})
        params = @options.merge(opts)

        self.class.get("/jobs/#{job_id}/profile", params)
      end

      def delete(job_id, opts={})
        params = @options.merge(opts)
        self.class.delete("/jobs/#{job_id}", params)
      end

      def sync_schema(job_id, opts={})
        params = @options.merge(opts)

        self.class.put("/jobs/#{job_id}/sync/schema", params)
      end

    end

  end
end
