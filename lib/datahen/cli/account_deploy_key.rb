module Datahen
  class CLI < Thor
    class AccountDeployKey < Thor
      package_name "account deploy_key"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "show", "Show public deploy key"
      def show()
        client = Client::DeployKey.new()
        puts "#{client.find()}"
      end

      desc "recreate", "Recreate public deploy key"
      long_desc <<-LONGDESC
        Recreate public deploy key.
      LONGDESC
      def recreate()
        client = Client::DeployKey.new()
        puts "#{client.create()}"
      end
    end
  end

end
