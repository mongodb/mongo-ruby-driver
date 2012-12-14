module Mongo
  module ReadPreference
    READ_PREFERENCES = [
      :primary,
      :primary_preferred,
      :secondary,
      :secondary_preferred,
      :nearest
    ]

    MONGOS_MODES = {
      :primary              => :primary,
      :primary_preferred    => :primaryPreferred,
      :secondary            => :secondary,
      :secondary_preferred  => :secondaryPreferred,
      :nearest              => :nearest
    }

    def self.mongos(mode, tag_sets)
      if mode != :secondary_preferred || !tag_sets.empty?
        mongos_read_preference = BSON::OrderedHash[:mode => MONGOS_MODES[mode]]
        mongos_read_preference[:tags] = tag_sets if !tag_sets.empty?
      end
      mongos_read_preference
    end

    def self.validate(value)
      if READ_PREFERENCES.include?(value)
        return true
      else
        raise MongoArgumentError, "#{value} is not a valid read preference. " +
          "Please specify one of the following read preferences as a symbol: #{READ_PREFERENCES}"
      end
    end

    def select_pool(mode, tags, latency)
      if mode == :primary && !tags.empty?
        raise MongoArgumentError, "Read preferecy :primary cannot be combined with tags"
      end

      case mode
        when :primary
          primary_pool
        when :primary_preferred
          primary_pool || select_secondary_pool(secondary_pools, tags, latency)
        when :secondary
          select_secondary_pool(secondary_pools, tags, latency)
        when :secondary_preferred
          select_secondary_pool(secondary_pools, tags, latency) || primary_pool
        when :nearest
          select_secondary_pool(pools, tags, latency)
      end
    end

    def select_secondary_pool(candidates, tag_sets, latency)
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

      matches.empty? ? nil : select_near_pool(matches, latency)
    end

    def select_near_pool(candidates, latency)
      nearest_pool = candidates.min_by { |candidate| candidate.ping_time }
      near_pools = candidates.select do |candidate|
        (candidate.ping_time - nearest_pool.ping_time) <= latency
      end
      near_pools[ rand(near_pools.length) ]
    end
  end
end