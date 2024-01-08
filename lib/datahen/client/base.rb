require 'httparty'

module Datahen
  module Client
    class Base
      include HTTParty

      default_timeout 60

      DEFAULT_RETRY_LIMIT = {
        seeder: nil,
        parser: nil,
        finisher: nil
      }

      def self.env_auth_token
        ENV['DATAHEN_TOKEN']
      end

      def self.env_ignore_ssl
        ENV['DATAHEN_IGNORE_SSL'].to_s.strip == '1'
      end

      def self.random_delay max_seconds = 2
        (rand * max_seconds * 1000.0).to_i / 1000.0
      end

      def env_api_url
        ENV['DATAHEN_API_URL'].nil? ? 'https://app.datahen.com/api/v1' : ENV['DATAHEN_API_URL']
      end

      def ignore_ssl
        return @ignore_ssl unless @ignore_ssl.nil?
        @ignore_ssl = self.class.env_ignore_ssl
        @ignore_ssl
      end

      def auth_token
        @auth_token ||= self.class.env_auth_token
      end

      def auth_token= value
        @auth_token = value
      end

      def default_retry_limit
        @default_retry_limit ||= DEFAULT_RETRY_LIMIT.dup
      end

      def left_merge target, source
        # validate source and target
        return {} if target.nil? || !target.is_a?(Hash)
        return target if source.nil? || !source.is_a?(Hash)

        # left merge source into target
        target.merge(source.select{|k,v|target.has_key?(k)})
      end

      def retry times, delay = nil, err_msg = nil, stream = false
        limit = times.nil? ? nil : times.to_i
        delay = delay.nil? ? 5 : delay.to_i
        count = 0
        begin
          val = yield
          if stream
            return if val.nil?
            if val['error'] != ""
              raise StandardError.new(val['error'])
            end
          end
        rescue Error::CustomRetryError, StandardError => e
          is_custom_retry = e.is_a? Error::CustomRetryError
          real_delay = is_custom_retry ? e.delay : delay
          err_msg = is_custom_retry ? e.error : e.inspect
          
          STDERR.puts(err_msg)

          # wait before retry (default 5 sec)
          sleep(delay) if real_delay > 0

          # raise error when retry limit is reached
          raise e unless limit.nil? || count < limit

          # retry with a 100+ failsafe to prevent overflow error due integer limit
          should_aprox = limit.nil? && count > 99
          count += 1 unless should_aprox
          puts "#{err_msg.nil? ? '' : "#{err_msg} "}Retry \##{count}#{should_aprox ? '+' : ''}..."
          retry
        end
        val
      end

      def initialize(opts={})
        @ignore_ssl = opts[:ignore_ssl]
        self.class.base_uri(env_api_url)
        self.auth_token = opts[:auth_token] unless opts[:auth_token].nil?
        @options = {
          headers: {
            "Authorization" => "Bearer #{auth_token}",
            "Content-Type" => "application/json",
          },
          verify: !ignore_ssl
        }

        # extract and merge retry limits
        @default_retry_limit = self.left_merge(DEFAULT_RETRY_LIMIT, opts[:retry_limit])

        query = {}
        query[:p] = opts[:page] if opts[:page]
        query[:pp] = opts[:per_page] if opts[:per_page]
        query[:fetchfail] = opts[:fetch_fail] if opts[:fetch_fail]
        query[:parsefail] = opts[:parse_fail] if opts[:parse_fail]
        query[:status] = opts[:status] if opts[:status]
        query[:page_type] = opts[:page_type] if opts[:page_type]
        query[:url] = opts[:url] if opts[:url]
        query[:effective_url] = opts[:effective_url] if opts[:effective_url]
        query[:body] = opts[:body] if opts[:body]
        query[:parent_gid] = opts[:parent_gid] if opts[:parent_gid]
        query[:gid] = opts[:gid] if opts[:gid]
        query[:"min-timestamp"] = opts[:"min-timestamp"] if opts[:"min-timestamp"]
        query[:"max-timestamp"] = opts[:"max-timestamp"] if opts[:"max-timestamp"]
        query[:limit] = opts[:limit] if opts[:limit]
        query[:order] = opts[:order] if opts[:order]
        query[:filter] = opts[:filter] if opts[:filter]
        query[:force] = opts[:force] if opts[:force]
        query[:action] = opts[:action] if opts[:action]
        query[:"include-system"] = opts[:"include-system"] if opts[:"include-system"]

        if opts[:query]
          if opts[:query].is_a?(Hash)
            query[:q] = opts[:query].to_json
          elsif opts[:query].is_a?(String)
            query[:q] = JSON.parse(opts[:query]).to_json
          end
        end

        unless query.empty?
          @options.merge!(query: query)
        end
      end
    end
  end
end
