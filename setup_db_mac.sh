# Requires brew (https://brew.sh/)
# Tap MongoDB
brew tap mongodb/brew
# Install the database
brew install mongodb-community
# Start the database (including after reboot)
brew services start mongodb-community
# More info here (https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/)
