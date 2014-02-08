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
