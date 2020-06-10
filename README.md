# Flight delay analysis- Finding best time to fly

### Problem Statement
Since the invention of an airplane, people got a chance to travel around the globe fast and comfortable. Usually trips are planned in advance and flight time has
an impact on booking for accommodation or personal schedule. This means that, in the event of a delay, it costs money and frustration rearranging the
stay. This can be caused by a number of factors like carriers, weather, aircraft, etc. which leads to a certain time of a delay. Sadly, very few airlines inform
about the cause and so passengers are left waiting in uncertainty. By analyzing flight data from the recent years, assumptions for the reliability
of airlines can be made.

### Solution
There are thousands of flights in US every day and so to analyze it big data processing algorithms should be used. For this particular case we chose Spark
to work the data flow of flight on-time performance and causes in the US. The descriptive analysis shows flight delay patterns within years and airlines.

### Dataset used
The data selected for this project is taken from The U.S. Department of Transportation’s (DOT) Bureau of Transportation Statistics (BTS) [2]. It has a data
collection since June 2003 and updates it every month as soon as the Air Travel Consumer Report is released. It includes information about the date of flight,
origin airport, destination airport time delay, time performance, destination and causes. Data for each month is stored in a CSV file and full dataset is roughly
5.5 GB in size [3]. We will be using the data for the previous 5 years (since 2014) for this project.<br />
For the purposes to understand the data and demonstrate the outcome in comprehensive way, additional dataset was used. It contains all the IDs from
the US airlines corresponding to their names.

### Analysis method
Our goal is to get useful insights from the data and to understand it. The
flight performance data from the previous five years, brought us some questions
that we focused on:<br />
• When is the best time of day to fly with minimise delays?<br />
• Which is the best day of the week to fly with minimise delays?<br />
• Which is the best day of the month to fly with minimise delays?<br />
• Which is the best month of the year to fly with minimise delays?<br />
• Which airlines were the most/least reliable throughout the year?<br />
<br />

Since the dataset used was big in size CSV file, analyses required opensource distributed data streaming framework. We chose Spark for that reason.
Meanwhile, the output is also CSV file, but small in size, so we used Excel to
make visualizations. To conduct the experiment, the analyses was divided into
following steps.<br />

1. The datasets as CSV files were fetched and fed into the Spark. Cassandra
and Kafka were considered, but Spark seemed to be enough.<br />
2. The dimension reduction across the data was run to find the desired results. There were 8 methods read in DataFrame used:<br />
• time of day(flightDF) - times were ranged within an hour and number of delays (smaller than 30min and bigger than 30min)calculated;<br />
• day of week(flightDF) - all delays per week day calculated;<br />
• day of month(flightDF) - all delays per month day calculated;<br />
• month of year(flightDF) - all delays per month, per year calculated;<br />
• dep performance airline(flightDF) - all departure delays per airline calculate;<br />
• dep performance airport(flightDF) - all departure delays per airport calculate;<br />
• arr performance airline(flightDF) - all arrival delays per airline calculate;<br />
• arr performance airport(flightDF) - all arrival delays per airport calculate.<br />

## More info including the results in the report attached
