using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LinqToTwitter;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace ReadDaxTweets
{
    public static class ReadDaxTweets
    {
        [FunctionName("ReadDaxTweets")]
        public static void Run([TimerTrigger("0 45 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function execution saterted at: {DateTime.Now}");
            List<TweetForDB> Tweets = ReadTwitts(log);
            WriteTweets(log, Tweets);
            log.LogInformation($"C# Timer trigger function execution completed at: {DateTime.Now}");
        }

        public static List<TweetForDB> ReadTwitts(ILogger log)
        {
            List<TweetForDB> Tweets = new List<TweetForDB>();

            string consumerKey = Environment.GetEnvironmentVariable("TwitterConsumerKey");
            string consumerSecret = Environment.GetEnvironmentVariable("TwitterConsumerSecret");
            string accessToken = Environment.GetEnvironmentVariable("TwitterAccessToken");
            string accessTokenSecret = Environment.GetEnvironmentVariable("TwitterAccessTokenSecret");

            var auth = new SingleUserAuthorizer
            {
                CredentialStore = new InMemoryCredentialStore
                {
                    ConsumerKey = consumerKey,
                    ConsumerSecret = consumerSecret,
                    OAuthToken = accessToken,
                    OAuthTokenSecret = accessTokenSecret
                }
            };

            var context = new TwitterContext(auth);

            /////////////////////////////////////////////////////////////////////
            TwitterContext twitterCtx = new TwitterContext(auth);

            try
            {
                Tweets = DoPagedSearchAsync(twitterCtx).Result;
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.ToString());
            }

            return Tweets;
        }


        public static async Task<List<TweetForDB>> DoPagedSearchAsync(TwitterContext twitterCtx)
        {
            const int MaxSearchEntriesToReturn = 100;

            string searchTerm = "#DAX"; //+
                                        //"OR #DowJones" +
                                        //"OR #dowjonesindustrial";

            // oldest id you already have for this search term
            ulong sinceID = 1;

            // used after the first query to track current session
            ulong maxID;

            var combinedSearchResults = new List<Status>();

            List<Status> searchResponse =
                await
                (from search in twitterCtx.Search
                 where search.Type == SearchType.Search &&
                       search.Query == searchTerm &&
                       search.Count == MaxSearchEntriesToReturn &&
                       search.SinceID == sinceID
                 select search.Statuses)
                .SingleOrDefaultAsync();

            combinedSearchResults.AddRange(searchResponse);
            ulong previousMaxID = ulong.MaxValue;
            do
            {
                // one less than the newest id you've just queried
                maxID = searchResponse.Min(status => status.StatusID) - 1;

                Debug.Assert(maxID < previousMaxID);
                previousMaxID = maxID;

                searchResponse =
                    await
                    (from search in twitterCtx.Search
                     where search.Type == SearchType.Search &&
                           search.Query == searchTerm &&
                           search.Count == MaxSearchEntriesToReturn &&
                           search.MaxID == maxID &&
                           search.SinceID == sinceID
                     select search.Statuses)
                    .SingleOrDefaultAsync();

                combinedSearchResults.AddRange(searchResponse);
            } while (searchResponse.Any());

            List<TweetForDB> TweetsForDB = new List<TweetForDB>();

            foreach (var tweet in combinedSearchResults)
            {
                TweetForDB tweetDB = new TweetForDB();

                tweetDB.TweetId = tweet.StatusID;
                tweetDB.TweetCreatedAt = tweet.CreatedAt;
                tweetDB.ScreenName = tweet.User.ScreenNameResponse;
                tweetDB.Location = tweet.User.Location;
                tweetDB.Text = tweet.Text;
                tweetDB.FullText = tweet.FullText;
                tweetDB.Retweet = tweet.Retweeted;
                tweetDB.Verified = tweet.User.Verified;
                tweetDB.RetweetCount = tweet.RetweetCount;
                string Hashtags = string.Empty;
                tweet.Entities.HashTagEntities.ForEach(Hashtag =>
                    Hashtags += Hashtag.Text + ",");
                if (Hashtags != string.Empty)
                    Hashtags = Hashtags.Substring(0, Hashtags.Length - 1);
                tweetDB.Hashtags = Hashtags;
                tweetDB.FavoriteCount = tweet.FavoriteCount;
                tweetDB.FollowerCount = tweet.User.FollowersCount;

                TweetsForDB.Add(tweetDB);
            }

            return TweetsForDB;
        }

        public static void WriteTweets(ILogger log, List<TweetForDB> Tweets)
        {
            var SqlConnectionString = Environment.GetEnvironmentVariable("string-sqldb-information");

            StringBuilder sb;
            string sqlInput;

            List<ulong> TweetIdList = new List<ulong>();

            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {

                    sb = new StringBuilder();

                    sb.Append("SELECT [TweetId] FROM [Twitter].[Dax]");

                    sqlInput = sb.ToString();


                    using (SqlCommand command = new SqlCommand(sqlInput, connection))
                    {
                        connection.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TweetIdList.Add(MapLongToUlong(reader.GetInt64(0)));
                            }
                        }
                        connection.Close();

                    }
                }

            }
            catch (SqlException ex)
            {
                log.LogInformation(ex.ToString());
            }

            foreach (TweetForDB tweet in Tweets)
            {
                if (!TweetIdList.Contains(tweet.TweetId))
                {
                    try
                    {
                        using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                        {
                            sb = new StringBuilder();

                            sb.Append(@"INSERT INTO [Twitter].[Dax]([TweetId],[TweetCreatedAt],[ScreenName],[Location],[Text],[FullText],[Retweet],[Verified],[Hashtags],[RetweetCount],[FavoriteCount],[FollowerCount])
                                            VALUES (@TweetId,@TweetCreatedAt,@ScreenName,@Location,@Text,@FullText,@Retweet,@Verified,@Hashtags,@RetweetCount,@FavoriteCount,@FollowerCount)");

                            sqlInput = sb.ToString();

                            using (SqlCommand command = new SqlCommand(sqlInput, connection))
                            {
                                connection.Open();

                                command.CommandType = CommandType.Text;
                                command.Parameters.Clear();

                                command.Parameters.AddWithValue("@TweetId", new SqlInt64(MapUlongToLong(tweet.TweetId)));
                                command.Parameters.AddWithValue("@TweetCreatedAt", new SqlDateTime(tweet.TweetCreatedAt));
                                command.Parameters.AddWithValue("@ScreenName", new SqlString(tweet.ScreenName));
                                command.Parameters.AddWithValue("@Location", new SqlString(tweet.Location));
                                command.Parameters.AddWithValue("@Text", new SqlString(tweet.Text));
                                command.Parameters.AddWithValue("@FullText", new SqlString(tweet.FullText));
                                command.Parameters.AddWithValue("@Retweet", new SqlBoolean(tweet.Retweet));
                                command.Parameters.AddWithValue("@Verified", new SqlBoolean(tweet.Verified));
                                command.Parameters.AddWithValue("@Hashtags", new SqlString(tweet.Hashtags));
                                command.Parameters.AddWithValue("@RetweetCount", new SqlInt32(tweet.RetweetCount));

                                if (tweet.FavoriteCount.HasValue)
                                {
                                    command.Parameters.AddWithValue("@FavoriteCount", new SqlInt32(tweet.FavoriteCount.Value));
                                }
                                else
                                {
                                    command.Parameters.AddWithValue("@FavoriteCount", new SqlInt32(0));
                                }

                                command.Parameters.AddWithValue("@FollowerCount", new SqlInt32(tweet.FollowerCount));

                                command.ExecuteReader();

                                connection.Close();

                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        log.LogInformation(ex.ToString());
                    }
                }
            }

        }

        public static long MapUlongToLong(ulong ulongValue)
        {
            return unchecked((long)ulongValue + long.MinValue);
        }

        public static ulong MapLongToUlong(long longValue)
        {
            return unchecked((ulong)(longValue - long.MinValue));
        }

        public class TweetForDB
        {
            public ulong TweetId { get; set; }
            public DateTime TweetCreatedAt { get; set; }
            public string ScreenName { get; set; }
            public string Location { get; set; }
            public string Text { get; set; }
            public string FullText { get; set; }

            public bool Retweet { get; set; }
            public bool Verified { get; set; }
            public int RetweetCount { get; set; }
            public string Hashtags { get; set; }
            public int? FavoriteCount { get; set; }
            public int FollowerCount { get; set; }

        }
    }
}
