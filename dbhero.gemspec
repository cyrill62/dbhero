# frozen_string_literal: true

$LOAD_PATH.push File.expand_path('lib', __dir__)

# Maintain your gem's version:
require 'dbhero/version'

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = 'dbhero'
  s.version     = Dbhero::VERSION
  s.authors     = ['Antônio Roberto Silva']
  s.email       = ['forevertonny@gmail.com']
  s.homepage    = 'https://github.com/catarse/dbhero'
  s.summary     = 'Simple heroku dataclips report tool'
  s.description = 'Based on heroku dataclips feature, SQL -> Dataset'
  s.license     = 'MIT'

  s.files = Dir['{app,config,db,lib}/**/*', 'MIT-LICENSE', 'Rakefile',
                'README.rdoc']
  s.test_files = Dir['spec/**/*']

  s.add_dependency 'has_scope'
  s.add_dependency 'rails', '> 5.0.0', '< 9.0.0'
  s.add_dependency 'responders'
  s.add_dependency 'sass-rails'
  s.add_dependency 'slim-rails'

  s.add_development_dependency 'factory_bot_rails'
  s.add_development_dependency 'pg'
  s.add_development_dependency 'rspec-rails', '~> 3.2'
  s.add_development_dependency 'rubocop-rails'
  s.add_development_dependency 'rubocop-rspec'
  s.add_development_dependency 'shoulda-matchers'
end
