# frozen_string_literal: true

Rails.application.routes.draw do
  mount Dbhero::Engine => '/dbhero', as: 'dbhero'
end
