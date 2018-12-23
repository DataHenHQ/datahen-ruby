module AnswersEngine
  module Client
    class Scraper < AnswersEngine::Client::Base

      def find(scraper_name)
        self.class.get("/scrapers/#{scraper_name}", @options)
      end

      def all(opts={})
        self.class.get("/scrapers", @options)
      end

      def create(scraper_name, git_repository, opts={})
        body = {
            name: scraper_name,
            git_repository: git_repository,
            git_branch: opts[:branch] ? opts[:branch] : "master"}

        body[:freshness_type] = opts[:freshness_type] if opts[:freshness_type]
        body[:force_fetch] = opts[:force_fetch] if opts[:force_fetch]
        body[:standard_worker_count] = opts[:workers] if opts[:workers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:disable_scheduler] = opts[:disable_scheduler] if opts[:disable_scheduler]
        body[:schedule] = opts[:schedule] if opts[:schedule]
        body[:timezone] = opts[:timezone] if opts[:timezone]
        @options.merge!({body: body.to_json})
        self.class.post("/scrapers", @options)
      end

      def update(scraper_name, opts={})
        body = {}

        body[:name] = opts[:name] if opts[:name]
        body[:git_repository] = opts[:repo] if opts[:repo]
        body[:git_branch] = opts[:branch] if opts[:branch]
        body[:freshness_type] = opts[:freshness_type] if opts[:freshness_type]
        body[:force_fetch] = opts[:force_fetch] if opts.has_key?("force_fetch")
        body[:standard_worker_count] = opts[:workers] if opts[:workers]
        body[:browser_worker_count] = opts[:browsers] if opts[:browsers]
        body[:proxy_type] = opts[:proxy_type] if opts[:proxy_type]
        body[:disable_scheduler] = opts[:disable_scheduler] if opts.has_key?("disable_scheduler")
        body[:schedule] = opts[:schedule] if opts[:schedule]
        body[:timezone] = opts[:timezone] if opts[:timezone]
        @options.merge!({body: body.to_json})

        self.class.put("/scrapers/#{scraper_name}", @options)
      end
    end
  end
end

