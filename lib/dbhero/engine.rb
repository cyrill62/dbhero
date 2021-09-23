# frozen_string_literal: true

class Dbhero::Engine < ::Rails::Engine
  isolate_namespace Dbhero

  config.generators do |g|
    g.template_engine :slim
  end
end
