import pandas as pd
import numpy as np
import pickle
import csv
import json
import multiprocessing as mp
def pickle_load(name):
    with open(name, 'rb') as f:
        data = pickle.load(f)
    return data

def pickle_save(data, name):
    with open(name, 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)

def extractCityData():
    business = pd.read_csv("business.csv", engine='python', encoding = "ISO-8859-1")
    
    li = business.values.tolist()
    selectData =list()
    d = business['city'].to_dict()

    for i in d:
        count = 0
        selectData =list()
        for j in business['city']:
            if(i == j):
                selectData.append(li[count])
            count+=1
        try:
            df = pd.DataFrame(selectData, columns=["business id","name","address","city","state","postal code","latitude","longitute","stars","review count","is opend","attributes","categories","hours"])
            df.to_csv(i+".csv", header='false')
        except:
            print(i)
            continue
    
def extractCertainCityData(city:str):
    business = pd.read_csv("business.csv", engine='python', encoding = "ISO-8859-1")
    # maxCity =str()
    # maxNum = 0
    # for i in business['city'].unique():
    #     temp = len(business[business['city'] == i])
    #     if temp > maxNum:
    #         maxNum = temp
    #         maxCity = i

    # print("{} , {}".format(maxCity,maxNum))
    selectData =business[business['city'] == city]
    selectData.to_csv(city+".csv", header='false')

def createUserCheckinData(city:str):
    reviews = pd.read_csv("review.csv", engine='python', encoding = "ISO-8859-1")
    citydata = pd.read_csv(city+".csv", engine='python', encoding = "ISO-8859-1")
    
    bsDict = dict()
    count =0
    for i in citydata["business_id"]:
        try:
            a = bsDict[i]
        except KeyError:
            bsDict[i] = count
        count += 1

    bsLen = len(citydata["business_id"])
    arr = np.array([[]])

    count = -1
    idx = 0
    li = reviews.values.tolist()
    userDict ={}
    selectData =list()
    userIdx = -1
    bsIdx = -1
    a =len(reviews["business_id"])
    for i in reviews["business_id"]:
        count+=1
        #해당 도시에 존재하는 가게인지를 확인
        try:
            bsIdx = bsDict[i]
        except KeyError:
            continue
        
        #이미 한번 나왔던 사용자인지 확인
        try:
            #이미 나온적 있는 사용자인 경우
            userIdx = userDict[li[count][0]]
            arr[userIdx][bsIdx] = 1.0

        except KeyError:
            #처음 나온 사용자인 경우
            userDict[li[count][0]] = idx
            temp = np.array([np.zeros(bsLen)])
            temp[0][bsIdx] = 1
            if arr.size > 0:
                arr = np.append(arr,temp,axis=0)
            else:
                arr = temp
            idx +=1
    #완료
    
    #방문데이터 저장
    pickle_save(arr,city+".pkl")
    

def createUserCheckinData_10(city:str):
    arr = pickle_load(city+".pkl")
    idx = 0
    l = len(arr)
    sumlist = arr.sum(axis = 1)
    for i in range(l):
        if sumlist[i] < 10:
            arr = np.delete(arr,(idx),axis=0)
        else:
            idx+=1

    idx = 0
    l = len(arr[0])

    sumlist = arr.sum(axis = 0)
    for i in range(l):
        if sumlist[i] < 1:
            arr = np.delete(arr,(idx),axis=1)
        else:
            idx+=1
    pickle_save(arr,city+"10.pkl")

def createCityReviewData(city:str):
    reviews = pd.read_csv("review.csv", engine='python', encoding = "ISO-8859-1")
    citydata = pd.read_csv(city+".csv", engine='python', encoding = "ISO-8859-1")
    citydata = citydata.drop(['Unnamed: 0', 'name', 'address', 'postal_code',  'stars', 'review_count','is_open', 'attributes', 'hours','city','state'],axis=1)
    #citydata = citydata.drop(['Unnamed: 0'],axis=1)
    print("citydata")
    print(citydata.columns)
    print("reviewdata")
    print(reviews.columns)
    citydata.to_csv(city+"_student.csv", header=False, index=False)
    reviews = reviews.drop(['review_id', 'stars', 'useful', 'funny','cool', 'date'], axis = 1)

    qu = (reviews['business_id']==citydata['business_id'][0])
    for i in citydata['business_id']:
        qu = qu | (reviews['business_id'] == i)

    newReviews = reviews.loc[qu]
    newReviews.to_csv(city+"Review_student.csv", header=False, index=False)
    
def csv2json(csvName:str, jsonName:str, fileIdNames):
    csvfile = open(csvName+'.csv','rt', encoding='utf-8')
    jsonfile= open(jsonName+'.json','w')
    reader = csv.DictReader(csvfile, fileIdNames)
    for row in reader:
        json.dump(row,jsonfile)
        jsonfile.write('\n')

def csvSampling(csvName):
    csvData = pd.read_csv(csvName+".csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    entireNum = len(csvData)
    csvData = csvData.sample(frac=1).reset_index(drop=True)
    aa = csvData['user_id'].unique()
    print(len(aa))
    qua = int(entireNum/4)
    work = [[csvData,aa[:qua],"0"],
    [csvData,aa[qua:qua*2],"1"],
    [csvData,aa[qua*2:qua*3],"2"],
    [csvData,aa[qua*3:],"3"]]

    pool = mp.Pool(processes=4)
    pool.starmap(samplingMultiProcess,work)
    pool.close()
    pool.join()
    
    
def samplingMultiProcess(csvData, aa, pp):
    print("{pp} start".format(pp = pp))
    testData = pd.DataFrame()
    trainData = pd.DataFrame()
    count =0
    for userId in aa:
        rows = csvData[csvData['user_id'] == userId]

        if len(rows)>1:
            testData = pd.concat([testData,rows.iloc[:int(len(rows)*0.4)]])
        trainData = pd.concat([trainData,rows.iloc[int(len(rows)*0.4):]])
        count+=1
        if count %1000 == 0:
            print("{pp} - {ss}".format(pp = pp, ss = count))
    testData.to_csv(pp+"_test.csv",header=False, index=False)
    trainData.to_csv(pp+"_train.csv",header=False, index=False)
    print("{pp} end".format(pp = pp))

def processing():
    testData = pd.read_csv("test.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    trainData = pd.read_csv("train.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'],header = None)

    print(testData)
    print(trainData)
    delUserList = []
    count = 0
    aa = testData['user_id'].unique()
    print(len(aa))
    for userID in aa:
        rows = trainData[trainData['user_id'] == userID]
        if len(rows) == 0:
            delUserList.append(userID)
        count+=1

    pickle_save(delUserList,"delUserList.pkl")
    testData.set_index('user_id')
    testData.drop(delUserList)

    testData.to_csv("test_01.csv",header=False, index=False)

def dataSampling():
    csvData = pd.read_csv("PhiladelphiaReview_student.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    entireNum = len(csvData)
    csvData = csvData.sample(frac=1).reset_index(drop=True)
    aa = csvData['user_id'].unique()
    print(len(aa))
    qua = int(len(aa)/4)
    work = [[csvData,csvData['user_id'].unique()[:qua],"0"],
    [csvData,csvData['user_id'].unique()[qua:qua*2],"1"],
    [csvData,csvData['user_id'].unique()[qua*2:qua*3],"2"],
    [csvData,csvData['user_id'].unique()[qua*3:],"3"]]

    pool = mp.Pool(processes=4)
    pool.starmap(samplingMultiProcess,work)
    pool.close()
    pool.join()
    # test0 = pd.read_csv("0_test.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # test1 = pd.read_csv("1_test.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # test2 = pd.read_csv("2_test.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # test3 = pd.read_csv("3_test.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)


    # train0 = pd.read_csv("0_train.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # train1 = pd.read_csv("1_train.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # train2 = pd.read_csv("2_train.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    # train3 = pd.read_csv("3_train.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)

    # mergedTest = pd.concat([test0,test1])
    # mergedTest = pd.concat([mergedTest,test2])
    # mergedTest = pd.concat([mergedTest,test3])

    # mergedTrain = pd.concat([train0,train1])
    # mergedTrain = pd.concat([mergedTrain,train2])
    # mergedTrain = pd.concat([mergedTrain,train3])

    # mergedTest.to_csv("merged_test.csv",header=False, index=False)
    # mergedTrain.to_csv("merged_train.csv",header=False, index=False)

if __name__ == "__main__":
    reviewData = pd.read_csv("./Yelp/data/Philadelphia_review.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/Philadelphia_business.csv", engine='python', encoding = "ISO-8859-1",names =['business_id','latitude', 'longitude','categories'], header = None)
    latitude_max = businessData['latitude'].max()
    latitude_min = businessData['latitude'].min()
    print(latitude_max)
    print(latitude_min)
    longitude_max = businessData['longitude'].max()
    longitude_min = businessData['longitude'].min()
    print(longitude_max)
    print(longitude_min)
