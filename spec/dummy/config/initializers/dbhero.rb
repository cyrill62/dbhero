# frozen_string_literal: true

Dbhero.configure do |config|
  # if you are using devise you can keep the "authenticate_user!"
  config.authenticate = true

  # Method to get the current user authenticated on your app
  # if you are using devise you can keep the "current_user"
  config.current_user_method = :current_user

  # uncomment to use custom user auth
  # config.custom_user_auth_condition = lambda do |user|
  #   user.admin?
  # end

  # String representation for user
  # when creating a dataclip just save on user field
  config.user_representation = :email
end
