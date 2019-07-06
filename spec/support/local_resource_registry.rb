require 'singleton'

class LocalResourceRegistry
  include Singleton

  def initialize
    @resources = []
  end

  def register(resource, finalizer)
    @resources << [resource, finalizer]
    # Return resource for chaining
    resource
  end

  def close_all
    @resources.each do |resource, finalizer|
      if finalizer.is_a?(Symbol)
        resource.send(finalizer)
      elsif finalizer.is_a?(Proc)
        finalizer.call(resource)
      else
        raise "Unknown finalizer: #{finalizer}"
      end
    end
    @resources = []
  end
end
