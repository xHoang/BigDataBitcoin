# Bitcoin analysis using big data jobs

Used Big Data Processing techniques - using Hadoop & Spark - to analyse a subset of the Bitcoin Blockchain (2009-
2014). Analysis includes finding top ten donors to the WikiLeaks Bitcoin wallet, and tracing coins that were
laundered in Ransomware attacks (up to 2nd generation ofwallets).


## Assignments

The assignment is split into three parts: A,B and C:


* Part A. Time Analysis (30%)

Part A wants us to aggregate every transaction in each month and then graph in a bar chart. In order
to achieve this, I have written code using the MRJob library as seen here:

![PartA](https://i.imgur.com/aq9abZK.png)

In order to complete this task, we need to understand that in the ‘transactions.csv’ file provided it
contains data which has five fields. In these fields, the third field contains the POSIX timestamp
which is the number of seconds since 1/1/1970.

To then convert this timestamp into a data without a year we use the datetime module where we
use the method fromtimestamp to convert the number of seconds into our local time (GMT).
However, the data returned is more than we need so we need to format it as we only want the year
and the month. So in order to do that we use strftime("%Y-%m") which basically formats the
output to only show the Year and month separated by a “-“.

We then yield the converted date and sent it to the reducer and combiner where they will total up
all the values where the job will then give out a file with the results. The results in my case split up
into parts and then merged back together. The data is found in outputRealMerge.txt

### Graph of Blockchain subset
In the given data we have an anomaly which shows the date 1970-01 and yields a value of one. This value is not technically an anomaly because it’s
referencing the genesis block in the blockchain, but it shows up as 1970-01 because the value from the field was 0. The dates were also not sorted so I have
sorted them using excel.
I have omitted this value from the graph:

![PartB](https://i.imgur.com/7Us5MOy.png)

From the graph shown above, the overall trend shown is that around 2012-06 transaction blocks
added on to the blockchain generally increase until 2012-12 where the transactions fall dramatically.
This maybe due to Bitcoin’s drop-in value from $819.43 per Bitcoin in February and then $379.37
later. However, more likely is that this is just an anomaly because it is rising in popularity despite its
downfall

* Part B. Top Ten Donors (40%)


* Part C Data Exploration (30%)

We have

## Built With

* Hadoop (Python / Java) - MRJob
* Spark (Python / Java)
*
