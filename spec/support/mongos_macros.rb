# frozen_string_literal: true
# rubocop:todo all

module MongosMacros

  class << self
    attr_accessor :distinct_ran
  end
  self.distinct_ran = {}

  # Work around for SERVER-39704 when seeing a Mongo::Error::OperationFailure
  # SnapshotUnavailable error -- run the distinct command on each mongos.
  def run_mongos_distincts(db_name, collection='test')
    MongosMacros.distinct_ran[db_name] ||= ::Utils.mongos_each_direct_client do |direct_client|
      direct_client.use(db_name)[collection].distinct('foo').to_a
    end
  end

  def maybe_run_mongos_distincts(db_name, collection='test')
    if ClusterConfig.instance.topology == :sharded
      run_mongos_distincts(db_name, collection)
    end
  end
end
