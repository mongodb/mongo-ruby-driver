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

    def read_preference
      {
        :mode => @read,
        :tags => @tag_sets,
        :latency => @acceptable_latency
      }
    end

    def read_pool(read_preference_override={})
      return primary_pool if mongos?

      read_pref = read_preference.merge(read_preference_override)

      if pinned_pool && pinned_pool[:read_preference] == read_pref
        pool = pinned_pool[:pool]
      else
        unpin_pool
        pool = select_pool(read_pref)
      end

      unless pool
        raise ConnectionFailure, "No replica set member available for query " +
          "with read preference matching mode #{read_pref[:mode]} and tags " +
          "matching #{read_pref[:tags]}."
      end

      pool
    end

    def select_pool(read_pref)
      if read_pref[:mode] == :primary && !read_pref[:tags].empty?
        raise MongoArgumentError, "Read preference :primary cannot be combined with tags"
      end

      case read_pref[:mode]
        when :primary
          primary_pool
        when :primary_preferred
          primary_pool || select_secondary_pool(secondary_pools, read_pref)
        when :secondary
          select_secondary_pool(secondary_pools, read_pref)
        when :secondary_preferred
          select_secondary_pool(secondary_pools, read_pref) || primary_pool
        when :nearest
          select_secondary_pool(pools, read_pref)
      end
    end

    def select_secondary_pool(candidates, read_pref)
      tag_sets = read_pref[:tags]

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

      matches.empty? ? nil : select_near_pool(matches, read_pref)
    end

    def select_near_pool(candidates, read_pref)
      latency = read_pref[:latency]
      nearest_pool = candidates.min_by { |candidate| candidate.ping_time }
      near_pools = candidates.select do |candidate|
        (candidate.ping_time - nearest_pool.ping_time) <= latency
      end
      near_pools[ rand(near_pools.length) ]
    end
  end
end
