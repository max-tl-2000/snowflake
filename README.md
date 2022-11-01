Reva Snowflake dbt project!

### Using the starter project

To run dbt from local machine run the following commands:

```
brew update
git -C "/usr/local/Homebrew/Library/Taps/homebrew/homebrew-core" fetch --unshallow
brew tap fishtown-analytics/dbt
brew install dbt
brew upgrade dbt
dbt --version
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Configuration file needed: `/.dbt/profiles.yml`
```
default:
  snowflake_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: zja29652.us-east-1

      # User/password auth
      user: TRANSFORMER_USER
      password: <<password>>

      role: TRANSFORMER
      database: ANALYTICS
      warehouse: TRANSFORMING
      schema: SNOW
      threads: 1
      client_session_keep_alive: False
      query_tag: dbt-vs-code
 ```
