from watchman.reddit_watcher import *
from watchman.twitter_watcher import *

# REDDIT
# Reddit Watcher init
reddit_watcher = RedditWatcher(personal_use_script='uyW2YVoyOR9WmQ', token='hnvRZFfSWC4ZZ6m3c8rz8n9L7-5CDg', username='LeLoMAu', password='Znajzh9lREgU')
# Get 10000 new posts from reddit communities
df_new_posts = reddit_watcher.get_new_posts(communities=['wallstreetbets', 'finance', 'StockMarket'], how_many_posts=10000)
# Get 100 new posts from reddit communities
df_hot_posts = reddit_watcher.get_hot_posts(communities=['wallstreetbets', 'finance', 'StockMarket'])

# TWITTER
# Twitter Watcher init
twitter_watcher = TwitterWatcher(bearer_token='AAAAAAAAAAAAAAAAAAAAABTyQAEAAAAAnUZy8U%2FGxnBSlssbEWZWHutNolQ%3DAk8TgQnMWAxKHMxuN5UClDwhLEKoOAfFYsn9qRQEZv3HYONfx4')
twitter_watcher.make_query(hashtags=['#stocks', '#stockmarket', '#investing', '#trading', '#finance', '#investment', '#wallstreet', '#StocksToWatch', '#StocksToBuy', '#stocksinfocus', '#stonks'])
df_tweets = twitter_watcher.get_results()
