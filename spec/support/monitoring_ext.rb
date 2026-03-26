# frozen_string_literal: true

module Mongo
  class Monitoring
    # #subscribers writes to the subscribers even when reading them,
    # confusing the tests.
    # This method returns only events with populated subscribers.
    def present_subscribers
      subs = {}
      subscribers.each do |k, v|
        subs[k] = v unless v.empty?
      end
      subs
    end
  end
end
