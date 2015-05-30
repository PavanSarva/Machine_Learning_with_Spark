#!/usr/bin/env python

import sys
import itertools
import datetime
import time
import numpy as np
from numpy import linalg as la

from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.stat import Statistics

def parseData(line):
    fields = line.strip().split(",")
    return fields

def setInput(fields):
    return long(fields[0]) % 10, (int(fields[2]), int(fields[10]), float(fields[21]))

def getItemList(fields):
    return int(fields[10]), fields[9]

def getGenreList(fields):
    return fields[13]

def getWeekDayIndex():
    if ( datetime.datetime.today().weekday() < 5):
        return 17
    else:
        return 16

def getCurrentViewTime():
    viewTime = ['Morning','Afternoon','Evening','Night']
    current_hour = time.localtime(time.time()).tm_hour 
    #print current_hour
    if (current_hour >= 4 and current_hour < 10):
        return viewTime[0]
    elif (current_hour >=10  and current_hour < 16):
        return viewTime[1]
    elif (current_hour >=16  and current_hour < 22):
        return viewTime[2]
    else:
        return viewTime[3]
   
def getCosineSimilarity(v1,v2):

    num = np.dot(v1,v2)
    den = la.norm(v1,2)*la.norm(v2,2)
    sim = 0
    if den:
      sim = (num/float(den))
    return sim    


def getProdItemRating(p,i):
    rating = np.dot(p,i)
    return rating


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))

    """
    print "begin predictions"
    pCount = predictions.count()
    for val in predictions.take(pCount):
        print val

    print "end predictions"

    dCount = data.count()

    for val in data.take(dCount):
        print val
    """

    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()

    """
    prCount = predictionsAndRatings.count()

    for val in predictionsAndRatings.take(prCount):
        print val
    """

    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print "Usage: /path/to/spark/bin/spark-submit --driver-memory 2g " + \
          "tvItemALS.py tvItemData_DirPath UserId"
        sys.exit(1)

    # set up environment
    conf = SparkConf() \
      .setAppName("TvProgram_RecoSystem_ALS") \
      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    tvItemData_DirPath = sys.argv[1]
    userId = sys.argv[2]
    current_view_time = getCurrentViewTime()
    weekdayIndex = getWeekDayIndex()
    data = sc.textFile(join(tvItemData_DirPath, "up_data.csv")).map(parseData).cache()
   
    userGenre = data.filter(lambda x: x[20] == current_view_time  and x[weekdayIndex] == "1" and x[2] == userId)\
      .map(getGenreList).distinct().cache()    
    userGenreList = userGenre.collect()
   
    ratings = data.filter(lambda x: x[20] == current_view_time and x[weekdayIndex] == "1").map(setInput)
    print "ViewTime is %s and WeekDayIndex is %d" %(current_view_time,weekdayIndex)
    ItemsGenre = data.filter(lambda x: x[20] == current_view_time  and x[weekdayIndex] == "1" \
      and x[13] in userGenreList).map(getItemList).distinct().cache();
    ItemListGenre = dict(ItemsGenre.collect())

    Items = data.filter(lambda x: x[20] == current_view_time  and x[weekdayIndex] == "1").map(getItemList)\
      .distinct().cache();
    ItemList = dict(Items.collect())

    numRatings = ratings.count()
    numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
    numItems = ratings.values().map(lambda r: r[1]).distinct().count()

    print "Got %d ratings from %d users on %d Items." % (numRatings, numUsers, numItems)

    numPartitions = 4

    training = ratings.filter(lambda x: x[0] < 6) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    test = ratings.filter(lambda x: x[0] >= 8).values().cache()

    userRatingsRDD = ratings.values().filter(lambda x: x[0] == int(userId)).repartition(numPartitions).cache()
    userRatings = userRatingsRDD.sortBy(lambda x: x[2], ascending=False).collect()
    recoItemId = userRatings[0][1]

    #print recoItemId

    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()

    print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)

    # train models and evaluate them on the validation set

    ranks = [4, 8,12,16,20,24]
    lambdas = [0.1, 10.0, 0.01, 0.5]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lmbda)
        #model = ALS.trainImplicit(training, rank, numIter, lmbda)
        validationRmse = computeRmse(model, validation, numValidation)
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    testRmse = computeRmse(bestModel, test, numTest)

    # evaluate the best model on the test set
    print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
      + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)

    # compare the best model with a naive baseline that always returns the mean rating
    meanRating = training.union(validation).map(lambda x: x[2]).mean()
    baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
    improvement = (baselineRmse - testRmse) / baselineRmse * 100
    print "The best model improves the baseline by %.2f" % (improvement) + "%."

    # make personalized recommendations
    userRatedItemIds = set([x[1] for x in userRatings])
    candidates = sc.parallelize([m for m in ItemList if m not in userRatedItemIds])
    print candidates.first()

    predictions = bestModel.predictAll(candidates.map(lambda x: (int(userId), x))).collect()
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]
   
    #Listing recommended items with priority to Genre user prefers for the current time context
    print "Items recommended for user:"
    for i in xrange(len(recommendations)):
        if (recommendations[i][1] in ItemListGenre):
            print ("%2d: %s" % (i + 1, ItemListGenre[recommendations[i][1]])).encode('ascii', 'ignore')

    for i in xrange(len(recommendations)):
        if (recommendations[i][1] in ItemList and recommendations[i][1] not in ItemListGenre):    
            print ("%2d: %s" % (i + 1, ItemList[recommendations[i][1]])).encode('ascii', 'ignore')


    itemId = recoItemId
    itemVector = bestModel.productFeatures().lookup(itemId).pop()
    userVector = bestModel.userFeatures().lookup(int(userId)).pop()

    pFeatures = bestModel.productFeatures()
    pListFeatures = pFeatures.collect()

    uFeatures = bestModel.userFeatures()    
    uListFeatures = uFeatures.collect()

    similerItemsRDD = pFeatures.map(lambda (x,y): (x,getCosineSimilarity(itemVector,y))).cache()

    """
    #The item item correlation could also obtained via other similarity measures such as 'Pearson' using 
    #Statistics.corr() method as below

 
    first_product = bestModel.productFeatures().take(1)[0]
    itemVectorRDD = sc.parallelize(itemVector)
    neighboutItemRDD = sc.parallelize(first_product[1])
    print "Pearson corr:"
    print Statistics.corr(itemVectorRDD,neighboutItemRDD)

    """
    
    simItemProdRating = pFeatures.filter(lambda (x,y): x not in userRatedItemIds)\
      .map(lambda (x,y): (x, getProdItemRating(userVector,y))).cache()
    simItemProdRatingCount = simItemProdRating.count()

    simItemProdRatingList = simItemProdRating.sortBy(lambda x: x[1],ascending= False).collect()
    simItemProdRatingRecoList = simItemProdRatingList[:50]

    #Items from the dot product of the user's product vector and items Vector 
    #corresponding to the similar items to arrive at ratings for the user 
    print "Recommended items sorted by estimated rating for items similar to the most preferred item from user"
    for i in xrange(len(simItemProdRatingRecoList)):
        if (simItemProdRatingRecoList[i][0] in ItemListGenre):
            print ("%2d: %s" % (i + 1, ItemListGenre[simItemProdRatingRecoList[i][0]])).encode('ascii', 'ignore')

    for i in xrange(len(simItemProdRatingRecoList)):
        if (simItemProdRatingRecoList[i][0] in ItemList and simItemProdRatingRecoList[i][0] not in ItemListGenre):    
            print ("%2d: %s" % (i + 1, ItemList[simItemProdRatingRecoList[i][0]])).encode('ascii', 'ignore')


    # Items similar to the most preferred item by user are sorted and top 5o items from
    # the list are recommended
    print "Recommended items that are highly similar to the most preferred item from user"
    sortedSimItemList = sorted(similerItemsRDD.top(50),key = lambda x: x[1], reverse = True)

    for i in xrange(len(sortedSimItemList)):
        if (sortedSimItemList[i][0] in ItemListGenre):
            print ("%2d: %s" % (i + 1, ItemListGenre[sortedSimItemList[i][0]])).encode('ascii', 'ignore')

    for i in xrange(len(sortedSimItemList)):
        if (sortedSimItemList[i][0] in ItemList and sortedSimItemList[i][0] not in ItemListGenre):    
            print ("%2d: %s" % (i + 1, ItemList[sortedSimItemList[i][0]])).encode('ascii', 'ignore')

    # clean up
    sc.stop()
