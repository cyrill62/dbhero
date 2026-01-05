# frozen_string_literal: true

require File.expand_path('boot', __dir__)

require 'rails/all'

Bundler.require(*Rails.groups)
require 'dbhero'
require 'slim'

module Dummy
  class Application < Rails::Application
    config.generators do |g|
      g.template_engine :slim
    end
  end
end
