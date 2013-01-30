#:nodoc:
module Mongo
  module ThreadLocalVariableManager
    def thread_local
      Thread.current[:mongo_thread_locals] ||= Hash.new do |hash, key|
        hash[key] = Hash.new unless hash.key? key
        hash[key]
      end
    end
  end
end