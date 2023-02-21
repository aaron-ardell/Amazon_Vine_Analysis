# Amazon_Vine_Analysis
## Overview

After our previous successful project for the company SellBy, they've tasked us with another larger project: analyzing Amazon reviews written by members of the paid Amazon Vine program. The Amazon Vine program is a service that allows manufacturers and publishers to receive reviews for their products. Companies like SellBy pay a small fee to Amazon and provide products to Amazon Vine members, who are then required to publish a review.

In this project, we'll select a dataset containing reviews of of products in the music category. Using PySpark we'll perform the ETL process extracting the dataset, transforming the data, connecting to an AWS RDS instance and loading the transformed data into a pgAdmin. Finally, using PySpark, we'll determine if there is any bias towards favorable reviews from Vine members in your dataset.

## Process

### ETL

First, we'll install the latest version of Spark, set environment variables, download a Postgres driver that will allow Spark to interact with Postgres and start the Spark Session. Once that's complete, our next step will be loading the dataset from [this webpage](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Music_v1_00.tsv.gz). It's a large dataset with 4751577 rows of data with 15 columns.

```
from pyspark import SparkFiles
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Music_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8").csv(SparkFiles.get("amazon_reviews_us_Music_v1_00.tsv.gz"), sep="\t", header=True, inferSchema=True)
df.show()
```

We've been charged with dividing the dataset into individual tables within the Postgresql server in the following table format:

```
CREATE TABLE review_id_table (
  review_id TEXT PRIMARY KEY NOT NULL,
  customer_id INTEGER,
  product_id TEXT,
  product_parent INTEGER,
  review_date DATE -- this should be in the formate yyyy-mm-dd
);

-- This table will contain only unique values
CREATE TABLE products_table (
  product_id TEXT PRIMARY KEY NOT NULL UNIQUE,
  product_title TEXT
);

-- Customer table for first data set
CREATE TABLE customers_table (
  customer_id INT PRIMARY KEY NOT NULL UNIQUE,
  customer_count INT
);

-- vine table
CREATE TABLE vine_table (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);
```

Starting with the review table, one of the challenges is changing the format of the reveiw_date column into the proper datetime format while selecting the proper columns for the review_id table. 

```
review_id_df = df.select(["review_id", "customer_id", "product_id", "product_parent", to_date("review_date", 'yyyy-MM-dd').alias("review_date")])
review_id_df.show(5)
```

The products table needs to only contain unique values. The `drop_duplicates()` function will take care of that.

```
products_df = df.select(["product_id", "product_title"]).drop_duplicates()
products_df.show()
```

The customers_table is going to involve a `count` function making up a new column that doesn't currently exist in the original dataset. Using `groupby` we can isolate the customer_id column, perform out `count` function and use the PySpark `withColumnRenamed` to ensure our created column has the correct title.
 
 ```
customers_df = df.groupby("customer_id").agg({"customer_id":"count"}).withColumnRenamed("count(customer_id)", "customer_count")
customers_df.show()
```

The vine_table is the most straight forward, using `select` to pick out the designated columns into the DataFrame.

```
vine_df = df.select(["review_id", "star_rating", "helpful_votes", "total_votes", "vine", "verified_purchase"])
vine_df.show(5)
```

With that, the DataFrames are complete. Now it's only a process of configuring our RDS variables in preparation to loading the DataFrames into the RDS with the proper table names. Here's an example of the review_id_table.

```
review_id_df.write.jdbc(url=jdbc_url, table='review_id_table', mode=mode, properties=config)
```

And verify that the table uploaded successfully to the RDS, by opening the table contents in PGAdmin on a linked server.

![This is an image](https://github.com/aaron-ardell/Amazon_Vine_Analysis/blob/main/review_id_table.png) 

### Bias Review

We start with the vine_df DataFrame from our ETL process and focus on the relationship between the reviews of paid and nonpaid Vine members. With 4751577 lines of data, we'll want reign in the scope of our analysis to the best examples of what we're trying to analyze. For instance, let's not have single vote reviews, we'll set a threshold of at least 20 total votes to remain in our dataset using a simple `filter`.

```
greater_than_twenty_df = filtered_df.filter(filtered_df['total_votes'] >= 20)
greater_than_twenty_df.show(5)
```

Now we've streamlined our data to 153157 lines of reviews that have a healthy amount of raw votes. But we're focusing on the best performing reviews and the potential bias created towards them. Thus, we're comparing the highest performing reviews. That's another filter added to the process to bring the successful reviews to the forefront.

```
greater_fifty_percent_df = greater_than_twenty_df.filter(greater_than_twenty_df['helpful_votes']/ greater_than_twenty_df['total_votes'] >= 0.5)
greater_fifty_percent_df.show(5)
```

Now we're at 105986 reviews with a reasonable amount of votes and greater than 50% helpful reviews. Now we can separate our Paid and Nonpaid Vine members.

```
vine_df = greater_fifty_percent_df.filter(greater_fifty_percent_df["vine"] == 'Y')
not_vine_df = greater_fifty_percent_df.filter(greater_fifty_percent_df["vine"] == 'N')
```

And here comes a problem with our particular dataset. We'll discuss that later.

For now, we'll finish up this analysis by calculating the final leg of our comparison.

```
# Number of Paid Vines
paid_vines = vine_df.count()
# Number of 5 star ratings amongst Paid Vines
paid_5_stars = vine_df.filter(vine_df["star_rating"] == '5').count()
# Percentage of 5 star ratings
percent_paid_vines = paid_5_stars / paid_vines

print(f"Total number of the Paid Vines: {paid_vines}")
print(f"Total number of the Paid 5 star review Vines: {paid_5_stars}")
print(f"Percentage of 5 star reviews amongst Paid Vines: {percent_paid_vines}")

# Number of Nonpaid Vines
nonpaid_vines = not_vine_df.count()
# Number of 5 star ratings amongst Nonpaid Vines
nonpaid_5_stars = not_vine_df.filter(not_vine_df['star_rating'] == '5').count()
# Percentage of 5 star ratings
percent_nonpaid_vines = nonpaid_5_stars / nonpaid_vines

print(f"Total number of not paid Reviews: {nonpaid_vines}")
print(f"Total number of the not paid 5 star rating review Vines: {nonpaid_5_stars}")
print(f"Percentage of 5 star reviews amongst not paid Vines: {percent_nonpaid_vines}") 
```

## Results

```
Total number of the Paid Vines: 7
Total number of the Paid 5 star review Vines: 0
Percentage of 5 star reviews amongst Paid Vines: 0.0
Total number of not paid Reviews: 105979
Total number of the not paid 5 star rating review Vines: 67580
Percentage of 5 star reviews amongst not paid Vines: 0.6376735013540419
```

- How many Vine reviews and non-Vine reviews were there?
  - This is the problem with this dataset, there were only 7 Paid Vine reviews left after filtering reviews with a signifigant vote total and favorable quantity of helpful reviews. Non-Vine reviews had over 105,979 reviews in the dataset.
- How many Vine reviews were 5 stars? How many non-Vine reviews were 5 stars?
  - There were 0 5-star Vine reviews. There were 67,580 non-Vine 5-star reviews.   
- What percentage of Vine reviews were 5 stars? What percentage of non-Vine reviews were 5 stars?
  - Vine reviews had 0% 5-star reviews. 5-star reviews comprised 63.77% of the total non-Vine reviews.

## Summary

Is there bias towards Paid Vines?

The US Music review category of Amazon Vines is an inappropriate dataset for analysis on this subject, there aren't enough paid Vines in this dataset to provide a signifigant comparison for the existance of bias. If we remove the filters that streamlined the dataset, then we'll be introducing a variety of issues that will flaw the integrity of our results. Combining this dataset's results with another dataset from a different category would be the most integral step forward.

Alternatively, an additional step could be to review the mean rating of Amazon Vine reviews with greater than 20 vote counts. This would remove the greater than 50% helpful reviews filter and add 45,000 reviews back to the analysis.  

![This is an image](https://github.com/aaron-ardell/Amazon_Vine_Analysis/blob/main/new_vine_df.png)

Unfortunately, it looks like even that step will fail to produce signifigant Paid Vines to provide a responsible comparison for bias. Picking a different category or combining these results with the results of another category would be the most appropriate path forward.
