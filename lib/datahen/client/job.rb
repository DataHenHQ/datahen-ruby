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
        body[:standard_worker_count] = opts[:workers] if opts[:workers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:profile] = opts[:profile] if opts[:profile]
        body[:max_page_size] = opts[:max_page_size] if opts[:max_page_size]
        body[:enable_global_cache] = opts[:enable_global_cache] if opts.has_key?("enable_global_cache") || opts.has_key?(:enable_global_cache)
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

        self.class.put("/jobs/#{job_id}/seeding_update", params)
      end

      def finisher_update(job_id, opts={})
        body = {}
        body[:outputs] = opts.fetch(:outputs) {[]}
        body[:finisher_status] = opts.fetch(:finisher_status){ nil }
        body[:log_error] = opts[:log_error] if opts[:log_error]

        params = @options.merge({body: body.to_json})

        self.class.put("/jobs/#{job_id}/finisher_update", params)
      end

      def profile(job_id, opts={})
        params = @options.merge(opts)

        self.class.get("/jobs/#{job_id}/profile", params)
      end

      def delete(job_id, opts={})
        params = @options.merge(opts)
        self.class.delete("/jobs/#{job_id}", params)
      end

    end

  end
end
