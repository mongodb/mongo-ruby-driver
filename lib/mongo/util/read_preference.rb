module Mongo
  module ReadPreference
    READ_PREFERENCES = [
      :primary,
      :primary_preferred,
      :secondary,
      :secondary_preferred,
      :nearest
    ]

    def self.validate(value)
      if READ_PREFERENCES.include?(value)
        return true
      else
        raise MongoArgumentError, "#{value} is not a valid read preference. " +
          "Please specify one of the following read preferences as a symbol: #{READ_PREFERENCES}"
      end
    end

    def select_read_pool(candidates, tag_sets, acceptable_latency)
      tag_sets = [tag_sets] unless tag_sets.is_a?(Array)

      if !tag_sets.empty?
        matches = []
        tag_sets.detect do |tag_set|
          matches = candidates.select do |candidate|
            tag_set.none? { |k,v| candidate.tags[k.to_s] != v } &&
            candidate.ping_time
          end
          !matches.empty?
        end
      else
        matches = candidates
      end

      matches.empty? ? nil : select_near_pool(matches, acceptable_latency)
    end

    def select_near_pool(pool_set, acceptable_latency)
      nearest_pool = pool_set.min_by { |pool| pool.ping_time }
      near_pools = pool_set.select do |pool|
        (pool.ping_time - nearest_pool.ping_time) <= acceptable_latency
      end
      near_pools[ rand(near_pools.length) ]
    end
  end
end