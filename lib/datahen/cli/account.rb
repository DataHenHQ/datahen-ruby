module Datahen
  class CLI < Thor
    class Account < Thor

      desc "profile", "displays the account applied profile"
      long_desc <<-LONGDESC
        Displays the account applied profile
      LONGDESC
      def profile()
        client = Client::Account.new(options)
        puts "#{client.profile()}"
      end

      desc "deploy_key SUBCOMMAND ...ARGS", "manage deploy key"
      subcommand "deploy_key", AccountDeployKey

    end
  end

end
