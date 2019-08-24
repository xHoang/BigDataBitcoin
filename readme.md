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

In order to complete this task, we need to understand that in the *transactions.csv* file provided it
contains data which has five fields. In these fields, the third field contains the POSIX timestamp
which is the number of seconds since 1/1/1970.

To then convert this timestamp into a data without a year we use the *datetime* module where we
use the method *fromtimestamp* to convert the number of seconds into our local time (GMT).
However, the data returned is more than we need so we need to format it as we only want the year
and the month. So in order to do that we use *strftime("%Y-%m")* which basically formats the
output to only show the Year and month separated by a *“-“*.

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

In this part I have used pyspark and I will explain my code in the 4 stages as proposed.

1. Initial filtering
At this stage all that is needed to be done is just to filter out wallets that are associated with the
Wikileaks bitcoin address. This was achieved in the following code snippet:

![PartB1](https://i.imgur.com/s59uPsN.png)

I first need to gather the input data, and this is provided to us with ‘vout.csv’. The data is separated
via commas and has five fields, so we need to check the lines in our file are valid. To do so we use
the following code snippet function.

![PartB2](https://i.imgur.com/qWFwD8h.png)

In line 2, I am calling *vout.filter(clean_vout)*. Using the filter function here is important as it
is used to return a new dataset (RDD) where the function given (*clean_vout*) returns true. Here we
just use a try-except case and firstly split up the data into 5 separate fields and check if the fourth
field contains the WikiLeaks address. If neither of any both cases are true, then it will return false
rendering the line read as invalid. Otherwise, it will format fields 2 and 3. I have chosen float for field
2 because it is the value of the bitcoin (and to provide precision) being sent and the other as float
just for computability sake. Technically it can be changed to int datatype, but this is not needed.

Finally on line three, we utilise anonymous functions by invoking *‘lambda‘* and we extract all the
values from that specific line via map. Map will return a new RDD dataset by passing each element
from the source through our function. In doing so, if we wanted to save the results it would output
all valid lines that contain wikileaks.

2. First replication join

![PartB3](https://i.imgur.com/GVFmuDE.png)

At this stage we need to compare all our valid vout outputs with vin in order to get all the data of the
associated transaction. Here, we do something very similar as above by first needing to filter it and I
do this by using the function cleaned_vin defined here:

![PartB4](https://i.imgur.com/2KjGL49.png)

This function is akin the *clean_vout* function above but instead we want to check if the lines in
the input are valid by checking if there are three fields and keeping precision on the vout field.

Starting at line 3, we still want to preserve our RDD in the form of (K , V) where K is our key and V is
our value. In this scenario, vin and vout both share the same associated transaction hash and so we
can inner join these files using the *join* function. This will create our data schema as depicted in
line 5 where our hash will be our join key. In doing so it returns a dataset of (K, (V, W)) pairs with all
pairs of elements for each hash. We now have all our data for the first join.

3. Second join

![PartB5](https://i.imgur.com/dj2LIbq.png)

At this stage we need to start grouping our data. Our first join has all the data, but it needs to be
queried and manipulated properly to yield results. Firstly, at line one, we want to create a new RDD,
so we will have a new join key. This join key will be (tx_hash, n). In our anonymous function we are
trying to access the tuples of our RDD. In order to understand this we should refer back to our RDD
schema: (hash, ((tx_hash, vout), (value, n, publicKey))). In *b[1][0][0]* the first tuple is referring to
the second file (the first values field) - (tx_hash, vout )The next is now referring to which value to be
used and since it’s 0 it’s the element – value. Finally, the first value inside this tuple, tx_hash.

In doing so, we get a new RDD with (tx_hash,n) as our join key. The values can be literally anything
since we will be grouping the RDD and will not be using the values section here.

We now want to compare this new RDD to the full v_out file which means considering all
transactions not just wikileaks. This is shown in lines 4 and 5. We then join these two RDD’s together
one again using *join*. The function to clean is shown here:

![PartB6](https://i.imgur.com/Iinw8Yl.png)

Notice when we join them together we will get the schema (hash,n), (value, publicKey), (“anything”).
We now need to group this data, so we can yield the publicKey and value in a pair. We can then give
these values to a reducer to aggregate the results. We do this by using the *reduceByKey(lambda
a,b: a+b)* transformation.

4. Top 10 donors

![PartB7](https://i.imgur.com/NM5XFo3.png)

Here, I use *takeOrdered* in order to get the values in descending order. This is done by adding the
“-“ in front of “x[1]. We then need to parallelise the output back into an RDD to we can use
*saveAsTextFile*.

This is outputted as at ext file shown here:

```

1 ('{17B6mtZr14VnCKaHkvzqpkuxMYKTvezDcp}', 46515.1894803)
2 ('{19TCgtx62HQmaaGy8WNhLvoLXLr7LvaDYn}', 5770.0)
3 ('{14dQGpcUhejZ6QhAQ9UGVh7an78xoDnfap}', 1931.482)
4 ('{1LNWw6yCxkUmkhArb2Nf2MPw6vG7u5WG7q}', 1894.3741862400002)
5 ('{1L8MdMLrgkCQJ1htiGRAcP11eJs662pYSS}', 806.13402728)
6 ('{1ECHwzKtRebkymjSnRKLqhQPkHCdDn6NeK}', 648.5199788)
7 ('{18pcznb96bbVE1mR7Di3hK7oWKsA1fDqhJ}', 637.04365574)
8 ('{19eXS2pE5f1yBggdwhPjauqCjS8YQCmnXa}', 576.835)
9 ('{1B9q5KG69tzjhqq3WSz3H7PAxDVTAwNdbV}', 556.7)
10 ('{1AUGSxE5e8yPPLGd7BM2aUxfzbokT6ZYSq}', 500.0)
```

**It is important to notice that we couldn’t tell specifically if the transactions inside the transactions all
were given to wikileaks. As such we are assuming that all the transactions sent are to wikileaks.**

![PartBFinal](https://i.imgur.com/yXnyBKm.png)

* Part C Data Exploration (30%)
```
Ransomware often gets victims to pay via bitcoin. Find wallet IDs involved in such attacks and
investigate how much money has been extorted. What happens to the coins afterwards?
```
![PartBC](https://i.imgur.com/XE0uw76.png)

Here I am checking how much money is being extorted by adding up all the values in the vout csv.
Essentially what is going on is we want to filter out our values through our function is_good_line
and in doing so we can get all valid lines. We then map out a new RDD with transactions containing
the public key of the crypto locker virus along with the bitcoin value sent. We then aggregate these
values and then output it.

Here we get: ('{18iEz617DoDp8CNQUyyrjCcC7XCGDf5SVb}', 45.9525) as our output.

As you can see the total amount extorted is 45.9525 bitcoins.

Next, we need to find out where the bitcoin has been transferred. I have completed this task by
firstly obtaining all the transaction id’s (tx_id) of all transactions where the crpytolocker virus was
the input. I have done this in the code snippet here:

![PartC](https://i.imgur.com/Ss6ePB4.png)

As you can see here, I have firstly filtered Vin to the point where the schema is: (tx_hash, tx_id). We
do not need the vout value from Vin as that’s associated with the previous transaction. We only
want to match with the transaction id only.

Next we filter Vout to the schema: (hash, (value,n,publickey)). Here the hash is now the join key and
by joining these two together we will get a new dataset in the form of (K, (V, W)). This schema is
(hash,tx_id,(value,n,publickey)) as shown in the commented section above.

Now that we have this data, we need to group this data for further joins, so we make tx_id the join
key in order to join with the full Vout file so that we will get all associated transaction with our
cryptolocker wallet address.

I then join these two files together and then group those values to yield only the wallet address sent
and their associated bitcoin

I have implemented this part here:

![PartC2](https://i.imgur.com/k0Rxlef.png)

So, for hash_vout_KV we want to make a new RDD where the join key will be the tx_id as described
above. I do this by accessing each tuple via an anonymous function lambda.

We then compare this RDD with the full Vout file in order to yield every transaction associated with
cryptolocker and aggregate those results.

The results produced were:

```
('{14tAJtuXQMEuoijbCLKTA1ZFYEnxLve7Cs}', 0.0)
('{1DQtfGxCHEuaQNB5aYkLYXCQbhK1aYrx3z}', 1.0)
('{1HhfYX12eqrctnwmty69HmHYF9k8JeF5Xn}', 0.0)
('{1JGQinkCrJafdXnSiCAZQHZ4eKuqwEdtFy}', 2.0)
('{1EqfeSJcFPZmfAvJ94ReB9RiQFpNAsZTDm}', 0.0)
('{1Dk55RHfPqmCiVNgTKBEW6TaKhtJi1s9mg}', 0.0)
('{1Cj7Zj5Mfp5Jh2186RgCfXCRUdkkrb2Toj}', 0.0)
('{1NrekWTaLcmGHirCz2cxmivTrysH4T8Fuq}', 1.0)
('{1CWV5ZtnMcTyeTfopL3RTbutYD8yDeHTBY}', 2.0)
('{14aEGh4PVzVMbTwQpH9LJYLNA2KC5TF67m}', 0.0)
('{14gkXH2QghqptYVV8By5E7ax6TxkwcmVo6}', 0.0)
('{174QBkXiqbU55fdrMmqskxKD4YVvvfU1c3}', 1.0)
('{17zs9DRa7fzpwq1r4EUucZuydPW6T8cQzo}', 1.0)
('{158hDbVrvoq4xWy7aBjywmruS2So8MUJQ3}', 0.0)
('{19Jo1iqmMMRTEGGEUKF165638qUF1cFUE2}', 0.0)
('{1Q3BLS7ZzcxZko9ZSGdF6bB7D9dmPomdwB}', 0.0)
('{1PWNEDAwg2FSdBGoPXbSxrBn52HcFPGbba}', 0.0)
('{1DngEQD6TwAGQ3AJDkFZ938xsvBBHDDPjS}', 0.0)
('{1P1WVKsjzanXqZfp3H9zrSLgrHAt4GvPe2}', 0.0)
('{1MhxtR7FojcbBnfni1wDiJ9nBZtTH6nfia}', 0.0)
('{19DQioRwoxzGZdEC6pwSC2tJJhzX3mtdD5}', 0.0)
('{146WJgh8qGPegf6WhyVZhPnaUfWJmnvDhk}', 4.0)
('{1HAtCJqvHxE81vNcLH85bdBBr5V58zpxgb}', 1.0)
('{18TbmQ42TZekptuQ3is3zwaPT1AdaJUkkr}', 0.0)
('{1GjGD3ZGy3is3rjFoYtAjuTbGVp1LXGGX7}', 0.0)
('{1L7SLmazbbcy614zsDSLwz4bxz1nnJvDeV}', 1.0)
('{1QA2HbHNrsmcPJv4C6m9ozfYND3ovgDghw}', 1.0)
('{1K77ZUKasJhrzfo1K2J1VKrTU58aRxwYU1}', 2.0)
('{19K2wtJryiGLJPwENGg85ahh9tP7DrrD2Z}', 2.0)
('{1Ed22j9VkasPPffJDZJY4bM5xTcYkaWPAT}', 0.0)
('{1FmC5Zfx4i5mJ6kx74FT23e2NPebtVpAgr}', 0.0)
('{1FGeak9FYVPABpea3X8uhttgEtWTnoUskH}', 1.0)
('{17D5PRGPXK59aiFXE6udYuVaiLfsoYyaEx}', 1.0)
('{1EZdbrwVuTtGZijezWmvvzDRjLmNVN2owm}', 0.0)
('{1FYdtN5cQadf9razLzhWaMJAbrsk95Jkjp}', 0.0)
('{1HBjuVs8Wkke3XKcYFrzzn2zxZhWzfifWG}', 3.0)
('{1BuyBXEZptfHf67c31Nud4jBJDvd11Z7Sq}', 1.0)
('{1NrzRt717fnioj48zkugLBvHmKhjp3DR84}', 0.0)
('{1883kBXg7FgH7Kj8WpMxRZD3Xt6ybxHisC}', 1.0)
('{1AEoiHY23fbBn8QiJ5y6oAjrhRY1Fb85uc}', 4.0)
('{17DBFo6CBkXVBW4ip2hVVesSbHHH2uup6x}', 1.0)
('{1JjUch6gcdJb7Puq1qb4Lgmnyr88QMkYBr}', 0.0)
('{1WosTAfwonpcBkts8nuvsWxtRrNkBWWf7}', 0.0)
('{1TR86vrQxw9Lv3iv6cHMB7wkcxkTxQdMy}', 0.0)
('{1CHLXhxY1kyfYvxdrNLMA1asbuSw4UxtEm}', 1.0)
('{1AQ1NyShspu9L15P7Zroyofy7vJFgZCbUy}', 1.0)
('{1G541ENwQBqG3WZgvYtVCojVgdHFpJ8RXs}', 0.0)
('{1GA17Pk7kC6bPLUQpeCZXmzwgjrXbUEWSD}', 1.0)
('{13BHAZXDcyGpTcGj5rjQgcfBk2Mn11zAQ4}', 0.0)
('{1LVzjiHqHeq6orVxfHmv8VdySNJsFzcUMf}', 1.0)
('{1HQJzS1kC9zhjxr7RGHjhvj1XuzCpzqLgb}', 0.0)
('{16N3jvnF7UhRh74TMmtwxpLX6zPQKPbEbh}', 1.0)
('{1HSN65qWF7EtBgrD85kjqeuYH7P5UaaSaZ}', 0.0)
```

*1AEoiHY23fbBn8QiJ5y6oAjrhRY1Fb85uc* is one of the larger values sending 4 worth of bitcoins from
this wallet to another known cryptolocker wallet. I believe cryptolocker was using multiple wallets in
order to obfuscate where the money may have come from via bitcoin laundering techniques.
These are then sent to multiple other address as seen here to make the money harder to track. If we
were to do another join, we will be able to see where the money is spent even further. There is also
a lot of transaction spending two bitcoins and this is because the cryptolocker developers asked for 2
BTC in ransom.

## Built With

* Hadoop (Python / Java) - MRJob
* Spark (Python / Java)
