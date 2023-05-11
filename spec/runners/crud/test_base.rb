# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module CRUD

    class CRUDTestBase

      # The test description.
      #
      # @return [ String ] description The test description.
      attr_reader :description

      # The expected command monitoring events.
      attr_reader :expectations

      def setup_fail_point(client)
        if @fail_point_command
          client.use(:admin).command(@fail_point_command)
        end
      end

      def clear_fail_point(client)
        if @fail_point_command
          ClientRegistry.instance.global_client('root_authorized').use(:admin).command(BSON::Document.new(@fail_point_command).merge(mode: "off"))
        end
      end

      private

      def resolve_target(client, operation)
        if operation.database_options
          # Some CRUD spec tests specify "database options". In Ruby there is
          # no facility to specify options on a database, hence these are
          # lifted to the client.
          client = client.with(operation.database_options)
        end
        case operation.object
        when 'collection'
          client[@spec.collection_name].with(operation.collection_options)
        when 'database'
          client.database
        when 'client'
          client
        when 'gridfsbucket'
          client.database.fs
        else
          raise "Unknown target #{operation.object}"
        end
      end
    end
  end
end
