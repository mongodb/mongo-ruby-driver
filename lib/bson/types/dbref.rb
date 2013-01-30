module BSON

  # A reference to another object in a MongoDB database.
  class DBRef

    attr_reader :namespace, :object_id

    # Create a DBRef. Use this class in conjunction with DB#dereference.
    #
    # @param [String] a collection name
    # @param [ObjectId] an object id
    #
    # @core dbrefs constructor_details
    def initialize(namespace, object_id)
      @namespace = namespace
      @object_id = object_id
    end

    def to_s
      "ns: #{namespace}, id: #{object_id}"
    end

    def to_hash
      {"$ns" => @namespace, "$id" => @object_id }
    end

  end
end
