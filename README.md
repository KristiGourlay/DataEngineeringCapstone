# Immigration and Demographics Capstone Project

This project consists of creating an ETL with Spark for a 2016 immigration data warehouse. In addition to immigration data, this project also incorporated demographics, temperature, and airports data. 


## Scope & Data

The objective of this project is to organize 4 datasets to better understand immigration patterns in the US. The four data sets are:

* I94 Immigration Data: This data is provided by the US National Tourism and Trade Office.
* Temperature Data: This data is provided via Kaggle and is originally used for climate change analysis. 
* US City Demographics: This data is provided by OpenSoft and contains data collected by the US Census Bureau in 2015.
* Airport Information: This is a table of airport codes and corresponding cities.

The end goal is a fully cleaned and organized schema that will be easy to aggregate, annalyze, and ask pertinent questions such as:

* Is there a correlation between temperature and numbers of immigrants per year?
* Are more diverse cities more likely to have increasing immigration numbers?
* Which airports recieve the highest number of immigrants on average?
* Is there a correlation between airports that recieve higher immigration numbers and proximity to cities with higher populations of foreign-born citizens?


Immigration Data: Looking at the sample data, we need to fix the arrival date formatting, and we see many null values associated with a number of columns. It is possible that some of this information can be dropped, but it is important to drill down into these columns to understand what they all represent. After inspection, the columns that will be used in the project will be:

* cicid: immigration id number, 
* i94yr 4 digit year that i94 is valid, 
* i94mon: numberic month , 
* i94port: the port of entry,
* arrdate: the arrival date,
* i94add: the state,
* i94bir: their age,
* i94visa: the type of visa,
* visapost: department of state when the the visa was issued,
* occup: the occupation of the person,
* biryear: the birth year of the person,
* gender


Temperature Data: The main issue with this data is the 354130 null values in both the AverageTemperature and the AverageTemperatureUncertainty columns. After drilling down into these specific null cases, it is clear that we can drop these columns as they do not provide any addition information that is not captured in other rows. Furthermore, the original use-case for this dataset, is an analysis for global warming; however, for our use-case we do not need the range of dates and range of geographical areas. We will limit our data to cities in the United States and dates equal to or later than 1990, to get an average monthly temperature for all listed cities. 

Demographics Data: At first glance, the data looks quite clean. There are no row duplicate values, and very few null values. Since there are multiple rows for cities (1 for each race), the number of nulls is actually lower than at first view. There is a data type that will need to be changed: count should be an integer and not a string. Also, it may be more efficient to perform a pivot on the data provided inorder to limit the amount of duplicated data. We have alot of duplicated data because we have 5 rows for Race and Count matched with 1 row for the rest of the data.  

Airport Codes Data: Airport Code data also looks pretty clean. There are many null values in the country column, but there are 0 nulls in iso_region, which we can use to create a column for country. We can also use the second part of iso_region, to create a state column. The iso_region appears to be a Country-State code. This will open up possibilities to join with our other data. Also, by splitting the coordinates column into latitude and longitude columns we allow for future cases where we would like to compare these to the temperature data which has those columns.


## Final Data Model

The final schema is a star schema. I chose this data model because it provides many benefits. Since this model has one fact table and four dimension tables, it provides the final user greater flexibility, allowing them to perform a variety of different queries. It necessitates less joins than a normalized transactional schema, and as result, is more efficient and query performance is much better. 


<p align="center">
<img src='static/images/capstone_diagram.jpg' width=600 height=500 alt='capstone_diagram'>
</p>


## Data Dictionary

### Dimension Tables:
Demographics Table:
   *   city_id: concatenation of city name and state code (primary key)
   *   city_name: name of the city 
   *   median_age: median age of population
   *   female_population: number of females in the city
   *   male_population: number of males in the city   
   *   total_population: the population of the city
   *   veteran_population: number of veterans in the city
   *   foreign_born_population: number of people in the city who are foreign born
   *   average_household_size: the average amount of people in a house
   *   state_code: state abreviation
   *   american_and_indian_alaskan_native_population: the number of people of American and Indian Alaska Native descent in the city
   *   asian_population: the number of people of Asian descent in the city
   *   black_or_african_population: the number of people of Black or African descent in the city
   *   hispanic_or_latino_population: the number of people of hispanic or latino descent

Temperature Table:
   *   temperature_id: primary key created
   *   year: year temperature taken
   *   month: month temperature taken
   *   city: the city where the temperature is taken
   *   country: the country that the city is in
   *   latitude: latitudinal position of the city
   *   longitude: longitudinal position of the city
   *   average_temperature: the average temperature over the last 30 years for that particular city in that particular month
   
Airport Table:
   *   airport_id: primary key
   *   name: name of the airport
   *   city: the city where the airport is located
   *   country_code: the country where the airport is located
   *   state_code: the state where the airport is located
   *   iata_code: the airport port code
   *   latitude: latitudinal position of the airport
   *   longitude: longitudinal position of the airport
   
Immigration Table:
   *   immigration_number: the immigration identificiation number for the person (primary key)
   *   age: the age of the person
   *   visa_type: the type of visa (student, business, etc.)
   *   birth_year: year the person was born
   *   gender: the gender that the person is.
   
### Fact Table

Immigration Fact Table:
   *   immigration_number: the immigration identificiation number for the person (primary key)
   *   city: the city where the the person immigrated to.
   *   state_code: the state where the city is located
   *   port: the port the person came in by
   *   month: the month the person arrived
   *   year: the year the person arrived



## Step in Process

1. Spin up a Spark Context
2. Clean raw data, create dimension tables, QA, and save to parquet:
    * demographics_table
        * create primary key by concatenating city name and state name
        * pivot table to use wide format for race and count instead of long format
        * select columns and alias if needed
        * run quality check to ensure no nulls and table exists
        * save to parquet for downstream query.
    * airport_table
        * split coordinates into 2 columns: longitude and latitude
        * split iso_region into 2 columns: country_code and state_code
        * get city name from iata_code with label mapping
        * select columns and alias if needed
        * run quality check to ensure no nulls and table exists
        * save to parquet for downstream query.
    * temperature_table
        * drop null values in Average Temperature column
        * filter and select only rows where the country is equal to United States and date is equal to or after 1990.
        * select columns and alias if needed
        * groupby to get average temperature per city per month
        * save to parquet for downstream query.
    * immigration_table
        * fix date format of arrival data
        * select columns and alias if needed
        * run quality check to ensure no nulls and table exists
        * save to parquet for downstream query.
3. Create Immigration fact table:
    * Create fact table with 5 columns of interest from immigration_staging_table 
    * Save to parquet for downstream query.
    

## Rationale Behind Data Structure and Future Direction

For this project, I decided to use Spark because it allows us to manipulate and transform big data and write the outputted structure to sql, json, or parquet files. The final schema is a star schema, because this allows all users to make more efficient data queries by necessitating less joins. Going forward this data can be done in batch processing, daily, weekly, or monthly intervals. The decision can be made based on the availability of the original data, but more importantly based on the needs of the final customer and the questions being asked. If we are asking questions like the questions posed above (in Scope and Data), weekly, or even monthly would be fine. Moving forward, this structure could be altered and re-structured based on the needs of the final customer. The following are examples:

What if the data was increased by 100x?
If the data was increaed by 100x, we could keep the same format, potentially increase the frequency of transformation. The most important thing we could do is spin up larger EC2 instances using Spark. This would give us greater capacity to handle the increased volume of data. 

What if the data populates a dashboard that must be updated on a daily basis by 7am every day?
In order to account for this outcome, we could schedule DAGs (for example, using Airflow) that would run every morning at 5am, to ensure that the data would be ready for the customer to view at 7am. It would be important to schedule warnings and to monitor to ensure that if the DAG was interrupted, we could investigate before final customers are effected. 

What if the database needed to be accessed by 100+ people?
We could either scale up the number of nodes/workers in our cluster, we could implement concurrent scaling in Redshift, or multi-cluster warehousing (in Snowflake), or we could potentially consider using a no-sql database like Apache Cassandra, where our schema conforms to the specified queries our customers need.


## To Run:

1. Enter personal AWS credentials into dl.cfg
2. Run python etl.py from the command line