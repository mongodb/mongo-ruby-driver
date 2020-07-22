# A Cursor that attempts to load documents from memory first before hitting
# the database if the same query has already been executed.

module Mongo
  class CachedCursor < Cursor

    # We iterate over the cached documents if they exist already in the
    # cursor otherwise proceed as normal.
    #
    # @example Iterate over the documents.
    #   cursor.each do |doc|
    #     # ...
    #   end
    #
    # @return [ Enumerator ] The enumerator.
    def each
      if @cached_docs
        @cached_docs.each do |doc|
          yield doc
        end
      else
        super
      end
    end

    # Testing purposes only
    def get_cached_docs
      @cached_docs
    end

    # Get a human-readable string representation of +Cursor+.
    #
    # @example Inspect the cursor.
    #   cursor.inspect
    #
    # @return [ String ] A string representation of a +Cursor+ instance.
    def inspect
      "#<Mongo::CachedCursor:0x#{object_id} @view=#{@view.inspect}>"
    end

    private

    def process(result)
      @remaining -= result.returned_count if limited?
      @cursor_id = result.cursor_id
      @coll_name ||= result.namespace.sub("#{database.name}.", '') if result.namespace
      documents = result.documents
      if @cursor_id.zero? && !@after_first_batch
        @cached_docs ||= []
        @cached_docs.concat(documents)
      end
      @after_first_batch = true
      documents
    end
  end
end
