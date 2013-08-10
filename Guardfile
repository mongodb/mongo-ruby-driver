# supresses issues with pry hooks on jruby
interactor :simple if RUBY_PLATFORM =~ /java/

guard 'bundler' do
  watch('Gemfile')
  watch(/^.+\.gemspec/)
end

guard 'rspec', :all_after_pass => false, :cli => '-t ~style' do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/(.+)\.rb$}) { |match| 'spec/#{match[1]}_spec.rb' }
  watch('spec/spec_helper.rb')  { 'spec' }
end

unless RUBY_VERSION < '1.9'
  guard :rubocop, :cli => '-f s' do
    watch(%r{(.+)\.rb$})
    watch(%r{Rakefile$})
    watch(%r{^lib/(.+)\.rake$})
    watch(%r{(?:.+/)?\.rubocop\.yml$}) { |m| File.dirname(m[0]) }
  end

  unless RUBY_PLATFORM =~ /java/
    guard 'yard', :port => 8808 do
      watch(%r{lib/.+\.rb})
    end
  end
end
