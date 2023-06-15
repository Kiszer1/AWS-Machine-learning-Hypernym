* Distributed System Programming: Scale Out with Cloud Computing.
* Hadoop.

* Made by:

Yaad Ben Moshe - 201648482

Nitzan Adam - 208871582


* Description :

The program will use the google Syntactic N-Grams corpus to train a classifier for automatic hypernym discovery
We will be using the implementation suggested in Rion Snow's paper : Learning syntactic patterns for automatic hypernym discovery.

* Running the App:
1. Place aws credentials in ~/.aws/credentials
2. All jar files Count.jar, Features.jar and Hypernym.jar already uploaded to hadoop bucket.
3. hypernym.txt file uploaded to hadoop bucket.
4. The configuration for the app could be changed in the Names file
5. DPMin value should be entred as an argument.
6. Run the local app / main.
7. Wait for app to finish, the program will creat and aff file consisting of the NounPairs as data and the depandency paths as features / attributes.
8. The output will be the Recall, Precision and F-Measure of a randomForest classifier.




* Process:

** Local App / Main:
1. Connects to AWS with credentials
2. Passes the DPMin as args.
3. Configure jobs : Features, NounPairs and Hypernym.
4. Configures a job flow and run it.
5. Wait for job flow to finish
6. Create aff file using the results of the job flow.
7. Classify the aff file using randomForest algorithem.
8. Output the Recall, Precision and F-Measure.


** Features:
1. Go over the syntactic N-grams and for each line input, check for nouns.
2. If more then 2 nouns exsist - find the shortest dependancy path between them.
3. Dependancy Paths are comprised of the stemmed words and depandency labels between 2 nouns.
4. For each dependancy path send from the mappers the path as key and the 2 nouns seprated by a space as value.
5. Each reducers will simply check if the path (the Key) has more then DPMin values (currently set to 3).
6. If the dependancy path has more then DPMin values, then emit the path.
7. After the job has finished the output will be in the file "features" in the bucket.
8. Merge output files into a single file called merged-features


*** Mapper:
1. Gets its input from google syntactic N-grams (Bigrams).
2. On setup, sets the tags for nouns in the corpus and the amount of indexes possible, for bigrams its 5.
3. For each line from the input 
   1. Check for nouns.
   2. If atleast 2 nouns are in the line, check for the shortest path between each 2 of them.
   3. For each path, if exsists, emit it as key with the 2 nouns as value.
4. If less then 2 nouns or no path between any nouns, emit nothing.

*** Reducer:
1. For each key which is a dependancy path, check if recieved atleast DPMin values.
2. If the path appeared atleast DPMin times, emit the path as Key and null as value.
3. Else emit nothing.


*** Memory :
1. Saved a hashSet with 4 values - the nounTags, O(1).
2. Saved a hashSet with 5 indexes - 0 to 4, O(1).
3. Saved a hashSet for indexes seen, max 5, O(1).
Total: O(1). 

*** Key-Value Pairs :
1. Mappers input keys : 16145663.
2. Mappers output keys : 3940032. 

1. Reducers input keys : 104440.
2. Reducers output keys : 14791, with DPMin set to 3.




** NounPairs:
1. Save the features from the output of the Count step.
2. Go over the syntactic N-grams and for each line input, check for nouns.
3. If more then 2 nouns exsist - find the shortest dependancy path between each 2 of them.
4. Dependancy Paths are comprised of the stemmed words and depandency labels between 2 nouns.
5. If such a path exsists, and its features is in the output of the Count step, (meaning it passed the DPMin) then:
6. Emit the 2 nouns as key and the path + the count as value.
7. Each reducers will simply check if the 2 nouns (the Key) has atleast 5 values as stated in the paper.
8. If the noun pair has more then 4 different dependancy paths, then emit the noun pair as key and all the features positions with their count.
9. After the job has finished, upload the amount of features to s3.
10. After the job has finished the output will be in the file "vectors" in the bucket.
 

*** Mapper:
1. Gets its input from google syntactic N-grams (Bigrams).
2. On setup, sets the tags for nouns in the corpus and the amount of indexes possible, for bigrams its 5.
3. Additionaly, on setup, saves the features from the Count step output with numbered positions starting from 0.
4. For each line from the input 
   1. Check for nouns.
   2. If atleast 2 nouns are in the line, check for the shortest path between each 2 of them.
   3. For each path, if exsists, check it is an output of the Count stage, meaning it passed the DPMin.
   4. for each of these paths, emit :
      1. The 2 nouns as key
      2. The path and the Count of times that noun with that path appeared in the corpus as value.
5. If less then 2 nouns or no path between any nouns that passed DPMin, emit nothing.
   		

*** Reducer:
1. For each key which is a noun pair, check if recieved atleast 5 values.
2. If the noun pair appeared atleast 5 times, emit the noun pair as Key and all its paths from the corpus with each paths count.
3. Else emit nothing.


*** Memory :
1. Saved an hashSet with 4 values - the nounTags, O(1).
2. Saved an hashSet with 5 indexes - 0 to 4, O(1).
3. Saved an hashSet for indexes seen, max 5, O(1).
4. Saved an hashMap with the features as keys and position as value.
   There are 14791 features, with bigger data sets the features can grow in size but up to a point.
   In the paper it reached 100,000 features which is still acceptable to save onto memory.
   We are using around 5 mb.
Total: 5-10mb of memory. 

*** Key-Value Pairs :
1. Mappers input keys : 16145663.
2. Mappers output keys : 3800085.

1. Reducers input keys : 3800085.
2. Reducers output keys : 14916.




** Hypernym:
1. Save hypernym to memory.
2. Save total features count to memory.
3. Go over the features stage output and for each line input, check if nouns pair is in the hypernym hashmap.
4. If hypernym is in the hashmap, emit the input line as key and the hypernyms value (true / false) as value.
5. At the reducer stage, create a vector for each key with size of features count and places the paths counts for the noun pair in the vector, rest 0.
6. Emit the vector with the true / false value at the end.
7. Merge output files into a single file called merged-vectors.

*** Mapper:
1. Gets its input from features stage output.
2. On setup, save the hypernym values to an hashmap with the noun pairs as key and true / false as value.
3. Additionaly on setup, get the features count from s3.
4. For each line from the input 
   1. Check if the noun pair is in the hypernym hashmap.
   2. If it is, emit the input line as key and the value from the hypernym hashmap (true / false).
5. If the noun pair isn't in the hypernym hashmap, emit nothing.

*** Reducer:
1. For each key which is a noun pair, create a vector with size of features count.
2. For each of the features in the key extract the count and position and set the vector in that position to count.
3. Emit the vector as string with the hypernym value (true / false) that came as the value.


*** Memory :
1. Saved a hashMap for the hypernym, with string of the noun pair and a string as value.
2. Saved a vector of features size integers.
   hypernym hashmap has around 150000 entries, therefore approximatly 5-10mb.
   vector with size of around 15000 integers, around 2-5mb.
   vectors shouildn't scale too much, with once again around 100000 entries at the paper.
Total: around 10-20mb. 

*** Key-Value Pairs :
1. Mappers input keys : 14916.
2. Mappers output keys : 217. 

1. Reducers input keys : 217.
2. Reducers output keys : 217.



* Results and logs at:
   s3://yaad-nitzan-hypernym-bucket
   s3://yaad-nitzan-hypernym-bucket/log/


* With RandomForest training algorithem:
   FMeasure: 0.7779074499046739
	Precision: 0.7736716688329591
	Recall: 0.8248847926267281

   Total Number of Instances              217
   Correctly Classified Instances         179
   Incorrectly Classified Instances        38

* Analysis :
1. 5 noun pairs with True-Positive :
   1. absorpt abil
   2. angl ab
   3. art abil
   4. attent abil
   5. ba ab
2. 5 noun pairs with False-Positive :
   1. bond-abil
   2. modul-abil
   3. comment-abil
   4. concept-abil
3. 5 noun pairs with True-Negative :
   1. eye-abil
   2. face-look
   3. faculti-abil
   4. famili-a
   5. penetr-abil
4. 5 noun pairs with False-Negative :
    1. charact-abil 
    2. christma-day
    3. equilibrium-abil
    4. expen-cost
    5. potenti-abil


* Stemmer : Snowball English stemmer.

* Training software : Weka.

* Classification Algorithem : RandomForest.

* Hadoop version : 2.10.1

* Instance type used: M4.Large
