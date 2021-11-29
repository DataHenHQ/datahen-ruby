require 'thor'
require 'datahen/scraper'
require 'datahen/cli/scraper_var'
require 'datahen/cli/scraper_exporter'
require 'datahen/cli/scraper_export'
require 'datahen/cli/scraper_job_var'
require 'datahen/cli/scraper_job'
require 'datahen/cli/scraper_finisher'
require 'datahen/cli/global_page'
require 'datahen/cli/scraper_page'
require 'datahen/cli/job_output'
require 'datahen/cli/job'
require 'datahen/cli/scraper_deployment'
require 'datahen/cli/scraper'
require 'datahen/cli/parser'
require 'datahen/cli/seeder'
require 'datahen/cli/finisher'
require 'datahen/cli/env_var'
require 'datahen/cli/account_deploy_key'
require 'datahen/cli/account'

module Datahen
  class CLI < Thor
    desc "scraper SUBCOMMAND ...ARGS", "manage scrapers"
    subcommand "scraper", Scraper

    desc "job SUBCOMMAND ...ARGS", "manage scrapers jobs"
    subcommand "job", Job

    desc "globalpage SUBCOMMAND ...ARGS", "interacts with global page"
    subcommand "globalpage", GlobalPage

    desc "parser SUBCOMMAND ...ARGS", "for parsing related activities"
    subcommand "parser", Parser

    desc "seeder SUBCOMMAND ...ARGS", "for seeding related activities"
    subcommand "seeder", Seeder

    desc "seeder SUBCOMMAND ...ARGS", "for seeding related activities"
    subcommand "finisher", Finisher

    desc "var SUBCOMMAND ...ARGS", "for environment variable related activities"
    subcommand "var", EnvVar

    desc "account SUBCOMMAND ...ARGS", "for account related activities"
    subcommand "account", Account
  end
end
