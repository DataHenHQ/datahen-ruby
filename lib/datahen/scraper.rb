require "datahen/scraper/parser"
require "datahen/scraper/batch_parser"
require "datahen/scraper/seeder"
require "datahen/scraper/finisher"
require "datahen/scraper/executor"
require "datahen/scraper/ruby_parser_executor"
require "datahen/scraper/ruby_seeder_executor"
require "datahen/scraper/ruby_finisher_executor"

module Datahen
  module Scraper
    # def self.list(opts={})
    #   scraper = Client::Scraper.new(opts)
    #   "Listing scrapers #{ENV['DATAHEN_TOKEN']} for #{scraper.all}"
    # end
  end
end
