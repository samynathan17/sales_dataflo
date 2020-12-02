# sales_dataflo
### Using the starter project

Try running the following commands:
- dbt run
- dbt test
- dbt deps → run package.yml file



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- https://www.startdataengineering.com/post/dbt-data-build-tool-tutorial/ ---good example
- https://rittmananalytics.com/blog/tag/dbt ---good example


model ->source folder= read snowflake warehouse data create staging views
model ->intermediate => read staging views do mapping,join,aggrigation then create intermediate views
model -> mart folder => read intermediate views then create a fact & dim tables

