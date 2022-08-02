# LSDS2021
## Large Scale Distributed Systems 2021 repository

Project 2 by Elias Asskali and Adam Dbouk
------------------------------------------------------------
### TwitterLanguageFilter
In this part, we took the files and got a JavaRDD in which we had a line for each tweet, after having the RDD we mapped each line to a Optional<SimplifiedTweet>, we filtered to get only the valid ones using isPresent and we mapped the optionals to SimplifiedTweet instances. Now we just had to filter again by language and we got an RDD with all the SimplifiedTweets corresponding to a language.
To write the output into a file we used saveAsTextFile and coalesce(1) to write the output in a single file, without this we got multiple files with the output.

Benchmarking (locally):
(lang = es) 

	Time: 153475 ms (2min 33s)
	  Tweets: 509435
	  
(lang = en) 

	Time: 147382 ms (2min 27s)
	  Tweets: 446604

(lang = fr) 

	Time: 142177 ms (2min 22s)
	  Tweets: 54909

If we compare this times (local) with the times from lab1 we can see a great improvement.
------------------------------------------------------------
### Benchmarking on EMR

After running all the steps we checked the info of each one and we obtained the following results:

	(lang = es) Time: 3.4 min
	(lang = en) Time: 3.3 min
	(lang = fr) Time: 3.1 min

We can see that it is taking more to run with Amazon's EMR than running locally, we suppose this is because EMR is using s3 and the speed of reading and writing in s3 is slower than the one for reading and writing in our local machine (using an SSD), thus, it is not fair to compare the benchmarking obtained locally with the one obtained with EMR. Also, the times depend on more factors that we are not taking into account.
------------------------------------------------------------
### BiGramsApp
In this case we parsed each tweet as in the previous exercice but this time we also filtered by non retweeted tweets (originals), after this we got the text and from this text we needed to get all the bigrams (normalised) so we created a method getBiGrams that splits the text, removes empty words, and with a for loop creates all the bigrams storing them in a Tuple2<String, String>, before storing the two words we used the method normalize which will simply trim, lowercase and remove some symbols from the start and end (the method could be better). The method biGrams returned a List with all the bigrams (tuples) found in the text. 
After this using mapToPair we added a 1 to each bigram and applied reduceByKey to get each bigram followed by the number of occurences.
Finally we needed to sort and take the first 10 elements but we could not use directly sortByKey as the number of occurences is not the key. For that, we swapped the javaPairRDD now having the # of occurrences as the key, we ordered (descending) and we swapped again the RDD getting now the bigrams ordered.
To write in the output we took the 10 first elements from the RDD getting a list, we converted this list to an rdd using parallelize and we used saveAsTextFile to write it in the output.

Benchmarking and results:
(lang = es):

	Time (--master local): 168235 ms (2min 48s)
	Time (--master master-url (4 cores)): 109531 ms (1min 49s)
	Time (EMR): 1 min
	Output:
	
		((de,la),3418)
		((#eurovision,#finaleurovision),2934)
		((que,no),2450)
		((la,canción),2447)
		((de,#eurovision),2270)
		((en,el),2183)
		((lo,que),2025)
		((a,la),1838)
		((en,#eurovision),1837)
		((en,la),1823)
		
(lang = en):

	Time (--master local): 178440 ms (2min 58s)
	Time (--master master-url): 124992 ms (2min 5s)
	Time (EMR): 1 min
	Output:
	
		((this,is),5823)
		((of,the),5791)
		((in,the),5225)
		((for,the),4370)
		((the,eurovision),4290)
		((eurovision,is),3337)
		((eurovision,song),3204)
		((i,love),3062)
		((is,the),2936)
		((song,contest),2740)
		
(lang = fr):

	Time (--master local): 157670 ms (2min 37s)
	Time (--master master-url): 106739 ms (1min 46s)
	Time (EMR): 1 min
	Output:
		((la,france),1312)
		((de,la),1285)
		((ce,soir),937)
		((je,suis),632)
		((la,chanson),610)
		((pour,la),607)
		((!,#eurovision),546)
		((à,la),438)
		((?,#eurovision),377)
		((y,a),370)
		
In the lang = fr case, we can see that some bigrams contain ! or ?, we did not handle this in normalize as we considered it correct.
Otherwise we would have added the symbols "!" and "?" to the list of characters we want to delete to normalize.
In terms of execution time we can see an improvement in speed when executing using spark as this way it is using a slave with 4 cores, the time benchmarking is not exact as it depends on many factors.
Also, as EMR here doesn't have to upload a large file to s3 we are getting better speeds than locally.
------------------------------------------------------------
### MostRetweetedApp
In the first place, we obtained two javaPairRDDs, one with the retweets each user has and one with the retweets each tweet has, we did not sort them because we've seen that when applying some operations it lost the order, so we decided to sort only at the end after all necessary operations are done.
After having this we joined both RDDs  having the tweetid as the key and (userid, #RtUser, #RtTweet) this was needed for the next step.
Finally, we joined to obtained RDD with an RDD containing all the tweets, this way, we obtained the tweet object having the id and also we made sure that all the tweets from the resulting join existed, so we did not have tweets from users who did not tweet during EV-2018. After this, we had to take the most retweeted tweet for the most retweeted users so we first reducedByKey making sure we ended up having only the most retweeted tweet for each user using a filter with ((a,b) -> a._2() >= b._2() ? a : b) now we only had to get the most retweeted users so we sorted the RDD by the number of tweets the user has. Now we only have to take 10 tweets and write them in the output file.
When coding, this gets complicated as the methods join, reduce, sort... need to have a certain element in the key so we had to apply a lot of times maptopair to realocate the variables inside the rdd and this way be able to use certain methods (the program could be more efficient). To make the code less complicated we added comments explaining why we do each step.

Benchmarking:

	Time (--master local): 516322ms (8min 36s)
	Time (Using a slave with 4 cores): 392351ms (6min 32s)
	Time (EMR): 4 min
	Output:
({"tweetId":995356756770467840,"text":"Ella está al mando. Con @PaquitaSalas nada malo puede pasar, ¿no? #Eurovision https://t.co/5HeUDCqxX6","userId":3143260474,"userName":"Netflix España","followersCount":573244,"language":"es","isRetweeted":false,"timestampMs":1526146518305}, Tweet retweets: 10809, User retweets: 10809)
({"tweetId":995372911920893953,"text":"Twenty minutes to gooo… #allaboard #eurovision https://t.co/brQoCRmrXI","userId":24679473,"userName":"BBC Eurovision🇬🇧","followersCount":188515,"language":"en","isRetweeted":false,"timestampMs":1526150369993}, Tweet retweets: 393, User retweets: 10155)
({"tweetId":995378197477736448,"text":"IT\u0027S HERE! IT\u0027S FINALLY HERE!🤩🤩#ESC2018 #AllAboard https://t.co/H4QjOio4a6","userId":15584187,"userName":"Eurovision","followersCount":435568,"language":"en","isRetweeted":false,"timestampMs":1526151630168}, Tweet retweets: 397, User retweets: 9864)
({"tweetId":995378833573531650,"text":"Empieza! ❤️❤️❤️\n#Eurovision\n#AmaiaAlfred12points","userId":437025093,"userName":"Manel Navarro Music","followersCount":15755,"language":"es","isRetweeted":false,"timestampMs":1526151781825}, Tweet retweets: 15, User retweets: 9678)
({"tweetId":995383476181401601,"text":"Already hate it 0/10 #Eurovision #esp","userId":39538010,"userName":"ƿ૯ωძɿ૯ƿɿ૯","followersCount":15961227,"language":"en","isRetweeted":false,"timestampMs":1526152888709}, Tweet retweets: 933, User retweets: 6831)
({"tweetId":995420352657461248,"text":"El dato: han desalojado a tres heterosexuales que se habían colado entre el público. #Eurovision #Eurocancion #YolaidayPacho12points","userId":38381308,"userName":"El Mundo Today","followersCount":1196214,"language":"es","isRetweeted":false,"timestampMs":1526161680746}, Tweet retweets: 528, User retweets: 6722)
({"tweetId":995408052886147079,"text":"Irlanda: Un puente, dos chiquitos que se quieren. #Eurovision https://t.co/B7ERrWdJdc","userId":739812492310896640,"userName":"Paquita Salas","followersCount":54630,"language":"es","isRetweeted":false,"timestampMs":1526158748252}, Tweet retweets: 141, User retweets: 5291)
({"tweetId":995372692902699009,"text":"Venga va, para no perder la tradición hoy habrá que tuitear un poquillo sobre Eurovision si o que mis panas.","userId":1501434991,"userName":"AuronPlay","followersCount":2764227,"language":"es","isRetweeted":false,"timestampMs":1526150317775}, Tweet retweets: 581, User retweets: 5167)
({"tweetId":995371907301208064,"text":"Pueden estar totalmente tranquilos Amaia y Alfred porque diría que España es el único país de todo Eurovision que c… https://t.co/75G6QPejYj","userId":2754746065,"userName":"Ibai","followersCount":295682,"language":"es","isRetweeted":false,"timestampMs":1526150130473}, Tweet retweets: 518, User retweets: 4640)
({"tweetId":995388112833531905,"text":"Cuando te baja la regla y se te ha olvidado el támpax #Eurovision https://t.co/1agMCdjbCo","userId":3260160764,"userName":"BuzzFeed España","followersCount":24289,"language":"es","isRetweeted":false,"timestampMs":1526153994173}, Tweet retweets: 144, User retweets: 4539)
