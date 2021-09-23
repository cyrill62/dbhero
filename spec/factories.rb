# frozen_string_literal: true

FactoryBot.define do
  factory :dataclip, class: 'Dbhero::Dataclip' do
    description do
      "Dummy query\nwich describes a dummy string and database version"
    end
    raw_query { "select 'dummy_foo' as dummy_bar, vesion() as db_version" }
    private { false }
  end
end
