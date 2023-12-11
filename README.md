# 📊 Read Twitter

This is a quick implementation to read tweets for with the hashtags of four different stock indices:

- DAX
- Dow Jones
- FTSE
- NASDAQ

The content thereof will be later on evaluated with sentiment analysis. The process runs as Azure functions.

## ⚙️ Functionality

The function retrieves periodically tweets from Twitter by using the free API that can run every 15 minutes. Each index is called once an hour with an offset of 15 minutes. The content is then stored in the database for furhter post-processing.

## 📜 Notes

This was part of a larger project that never went into production, so a cleaner implementation with a more TDD approach won't happen.

The project is being made public without the git history.