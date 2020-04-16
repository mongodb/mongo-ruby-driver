require 'mongo/operation/result'

require 'mongo/operation/shared/response_handling'
require 'mongo/operation/shared/executable'
require 'mongo/operation/shared/executable_no_validate'
require 'mongo/operation/shared/executable_transaction_label'
require 'mongo/operation/shared/polymorphic_lookup'
require 'mongo/operation/shared/polymorphic_result'
require 'mongo/operation/shared/read_preference_supported'
require 'mongo/operation/shared/bypass_document_validation'
require 'mongo/operation/shared/write_concern_supported'
require 'mongo/operation/shared/limited'
require 'mongo/operation/shared/sessions_supported'
require 'mongo/operation/shared/causal_consistency_supported'
require 'mongo/operation/shared/write'
require 'mongo/operation/shared/idable'
require 'mongo/operation/shared/specifiable'
require 'mongo/operation/shared/object_id_generator'
require 'mongo/operation/shared/op_msg_or_command'
require 'mongo/operation/shared/op_msg_or_find_command'
require 'mongo/operation/shared/op_msg_or_list_indexes_command'
require 'mongo/operation/shared/collections_info_or_list_collections'

require 'mongo/operation/op_msg_base'
require 'mongo/operation/command'
require 'mongo/operation/aggregate'
require 'mongo/operation/result'
require 'mongo/operation/collections_info'
require 'mongo/operation/list_collections'
require 'mongo/operation/update'
require 'mongo/operation/insert'
require 'mongo/operation/delete'
require 'mongo/operation/count'
require 'mongo/operation/distinct'
require 'mongo/operation/create'
require 'mongo/operation/drop'
require 'mongo/operation/drop_database'
require 'mongo/operation/get_more'
require 'mongo/operation/find'
require 'mongo/operation/explain'
require 'mongo/operation/kill_cursors'
require 'mongo/operation/indexes'
require 'mongo/operation/map_reduce'
require 'mongo/operation/users_info'
require 'mongo/operation/parallel_scan'
require 'mongo/operation/create_user'
require 'mongo/operation/update_user'
require 'mongo/operation/remove_user'
require 'mongo/operation/create_index'
require 'mongo/operation/drop_index'

module Mongo
  module Operation

    # The q field constant.
    #
    # @since 2.1.0
    Q = 'q'.freeze

    # The u field constant.
    #
    # @since 2.1.0
    U = 'u'.freeze

    # The limit field constant.
    #
    # @since 2.1.0
    LIMIT = 'limit'.freeze

    # The multi field constant.
    #
    # @since 2.1.0
    MULTI = 'multi'.freeze

    # The upsert field constant.
    #
    # @since 2.1.0
    UPSERT = 'upsert'.freeze

    # The collation field constant.
    #
    # @since 2.4.0
    COLLATION = 'collation'.freeze

    # The array filters field constant.
    #
    # @since 2.5.0
    ARRAY_FILTERS = 'arrayFilters'.freeze

    # The operation time field constant.
    #
    # @since 2.5.0
    OPERATION_TIME = 'operationTime'.freeze

    # The cluster time field constant.
    #
    # @since 2.5.0
    # @deprecated
    CLUSTER_TIME = '$clusterTime'.freeze
  end
end
