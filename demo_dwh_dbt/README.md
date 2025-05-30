Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Profile
location
```
%USERPROFILE%\.dbt\profiles.yml
```

content
```
demo_dwh_dbt:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: dwh
      host: NA                           # not used, but required by `dbt-core`
      server_side_parameters:
        "spark.sql.warehouse.dir": "../spark-data"
```
