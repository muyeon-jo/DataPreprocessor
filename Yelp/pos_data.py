import pandas as pd
import numpy as np
from haversine import haversine
import math
import multiprocessing as mp
import pickle
import matplotlib.pyplot as plt
import matplotlib.cm as cm
def pickle_load(name):
    with open(name, 'rb') as f:
        data = pickle.load(f)
    return data

def pickle_save(data, name):
    with open(name, 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def extractBusinessWithPos(max_latitude, max_longitude, min_latitude, min_longitude):
    business = pd.read_csv("./Yelp/data/business.csv", engine='python', encoding = "ISO-8859-1")
    business = business.drop(["state","city","name","address","postal_code","stars","review_count","is_open","attributes","hours"],axis=1)
    temp = business[business['latitude'] >=min_latitude]
    temp = temp[business['longitude']>=min_longitude]
    temp = temp[ business['latitude']<=max_latitude]
    specifics = temp[ business['longitude']<=max_longitude]
     

    print("specific business num: "+str(len(specifics)))
    specifics.to_csv("./Yelp/data/"+str(min_latitude)+" "+str(min_longitude)+" _ "+str(max_latitude)+" "+str(max_longitude)+".csv",header=True,index =False)

            
def getArea(business:str, review:str, size, minVisitedNum:int=1):
    """
    """
    reviewData = pd.read_csv("./Yelp/data/"+review+".csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/"+business+".csv", engine='python', encoding = "ISO-8859-1")
    print("review num: "+str(len(reviewData)))
    print("business num: "+str(len(businessData)))
    latitude_max = businessData['latitude'].max()
    latitude_min = businessData['latitude'].min()
    longitude_max = businessData['longitude'].max()
    longitude_min = businessData['longitude'].min()
    print("lat_max:{latmax} lat_min:{latmin}".format(latmax=latitude_max, latmin=latitude_min))
    print("lng_max:{lngmax} lng_min:{lngmin}".format(lngmax=longitude_max, lngmin=longitude_min))

    width1 = haversine((latitude_max,longitude_max),(latitude_max,longitude_min), unit="m")
    width2 = haversine((latitude_min,longitude_max),(latitude_min,longitude_min), unit="m")
    
    height1 = haversine((latitude_max,longitude_max),(latitude_min,longitude_max), unit="m")
    height2 = haversine((latitude_max,longitude_min),(latitude_min,longitude_min), unit="m")

    #지역 쪼개기
    colnum = int((width2+width1)/2/size)
    rownum = int(height1/size)
    delta = (longitude_max - longitude_min)/colnum
    areaRangeArr = np.zeros((rownum, colnum, 4))
    areaBusinessArr = dict()
    # for i in range(rownum):
    #     areaBusinessArr[i]=dict()
    #     for j in range(colnum):
    #         areaBusinessArr[i][j]=dict()

    # businessPos = dict()
    # for i in range(rownum):
    #     lat_min = latitude_min + (latitude_max - latitude_min)/rownum*(i)
    #     lat_max = latitude_min + (latitude_max - latitude_min)/rownum*(i+1)
    #     for j in range(colnum):
    #         lng_min = longitude_min + delta * j
    #         lng_max = longitude_min + delta * (j+1)

    #         areaRangeArr[i][j][0] = lat_min
    #         areaRangeArr[i][j][1] = lat_max
    #         areaRangeArr[i][j][2] = lng_min
    #         areaRangeArr[i][j][3] = lng_max

    #         temp = businessData[businessData['latitude']>=lat_min]
    #         temp = temp[temp['longitude']>=lng_min]
    #         if j == colnum-1:
    #             temp = temp[temp['longitude']<=lng_max]
    #         else:
    #             temp = temp[temp['longitude']<lng_max]

    #         if i == rownum-1:
    #             temp = temp[temp['latitude']<=lat_max]
    #         else:
    #             temp = temp[temp['latitude']<lat_max]

    #         #각 가게들이 어디 지역에 존재하는지 저장
    #         for index, data in temp.iterrows():
    #             businessPos[data['business_id']] = (i,j)
                
    #             temp = data['categories']
    #             if type(temp) == str:
    #                 temp = temp.split(", ")
    #                 areaBusinessArr[i][j][data['business_id']] = temp
    businessPos = pickle_load("./Yelp/data/"+business+" businessPos")
    areaRangeArr = pickle_load("./Yelp/data/"+business+" Area range")
    areaBusinessArr = pickle_load("./Yelp/data/"+business+" area list")


    userVisitData = dict()
    areaCategory = dict()
    visitedArea = np.zeros((rownum,colnum))
    for i in range(rownum):
        areaCategory[i]=dict()
        for j in range(colnum):
            areaCategory[i][j] = dict()
    tt = pickle_load("./Yelp/data/39.86492399 -75.651673 _ 40.247267 -74.8937988281 userVisitData")
    dropli = []
    for k,v in tt.items():
        if len(v) <minVisitedNum:
            dropli.append(k)

    for i in dropli:
        tt.pop(i)
    pickle_save(tt,"./Yelp/data/"+business+"_userVisitData")
    print("user ID len: {ul}".format(ul=len(tt)))
    #리뷰데이터 하나씩 읽으면서 사용자의 방문 데이터 작성
    count = 0
    lengthCheckDict = dict()
    for userID in tt:
        count +=1
        if count % 1000 == 0:
            print("now :"+str(count))
        for index, data in reviewData[reviewData['user_id'] == userID].iterrows():
            pos = businessPos[data['business_id']]
            lengthCheckDict[str(pos[0])+","+str(pos[1])]=1
            visitedArea[pos[0]][pos[1]] += 1
            temp = businessData[businessData['business_id']==data['business_id']]
            if pd.notnull(temp['categories']).sum() > 0:
                categories = temp['categories'].iloc[0]
                categories = categories.split(", ")

                for cg in categories:
                    try:
                        areaCategory[pos[0]][pos[1]][cg] += 1
                    except KeyError:
                        areaCategory[pos[0]][pos[1]][cg] = 1

    # pickle_save(areaRangeArr,"./Yelp/data/"+business+" Area range")
    # pickle_save(areaBusinessArr,"./Yelp/data/"+business+" area list")
    # pickle_save(businessPos,"./Yelp/data/"+business+" businessPos")
    print("dictionary length is : "+str(len(lengthCheckDict)))
    pickle_save(areaCategory,"./Yelp/data/"+business+"_areaCategory")
    pickle_save(visitedArea,"./Yelp/data/"+business+"_visitedArea")
    # review_len = len(reviewData)

def drawInGraphs(city:str):
    businessPos = pickle_load("./Yelp/data/"+city+" businessPos")
    userVisitData = pickle_load("./Yelp/data/"+city+" userVisitData")
    visitedArea = pickle_load("./Yelp/data/"+city+" visitedArea")
    areaCategory = pickle_load("./Yelp/data/"+city+" areaCategory")

    """
    사용자들이 방문한 가게들의 카테고리의 비율을 계산하고 이를 원형그래프로 표현
    """
    cateDict = dict()
    visitedX = []
    visitedY = []
    area = []
    color = []
    label = []
    N = 0
    for i,dd  in areaCategory.items():
        for j,value in dd.items():
            if len(value) > 0:
                sorted_dict = sorted(value.items(), key = lambda item: item[1], reverse = True)
                for q in sorted_dict:
                    try:
                        cateDict[q[0]] += q[1]
                    except KeyError:
                        cateDict[q[0]] = q[1]
                    N += q[1]
                visitedX.append(j)
                visitedY.append(i)
                area.append(sorted_dict[0][1])
                color.append(sorted_dict[0][1])
                label.append(sorted_dict[0][0])
    
    ratio = []
    labels = []
    sd = sorted(cateDict.items(), key = lambda item: item[1], reverse = True)
    count = 0
    for i in sd:
        if i[1]/N < 0.01:
            ratio.append((N-count)/N)
            labels.append("etc")
            break
        ratio.append(i[1]/N)
        labels.append(i[0])
        count +=i[1]
    plt.pie(ratio, labels=labels, autopct='%.1f%%')
    plt.show()


    """
    
    """
    # arr = np.zeros((425,645))
    ii = 0
    for category_label in labels:
        arr = np.zeros((425,645))
        for i in range(len(arr)):
            for j in range(len(arr[0])):
                if len(areaCategory[i][j])>0:
                    temp = sorted(areaCategory[i][j].items(), key = lambda item: item[1], reverse= True)
                    try:
                        x = areaCategory[i][j][category_label]
                    except KeyError:
                        continue
                    maxIdx = len(temp) - 1
                    if maxIdx > 4:
                        maxIdx = 4
                    if temp[maxIdx][1] <= x:
                        arr[i][j] = 10
        plt.imshow(arr, cmap=plt.get_cmap('inferno'))
        plt.colorbar()
        plt.show()  
    # plt.imshow(arr, cmap=plt.get_cmap('inferno'))
    # plt.colorbar()
    # plt.show()              
    """
    각 지역의 가게 수의 분포를 산점도로 표현
    """
    arr = np.zeros((425,645))
    visitedX = []
    visitedY = []
    area = []
    color = []
    for key, value in businessPos.items():
        arr[value[0]][value[1]]+=1
        
    for i in range(len(arr)):
        for j in range(len(arr[0])):
            if arr[i][j] > 0:
                visitedX.append(j)
                visitedY.append(i)
                area.append(arr[i][j])
                color.append(arr[i][j])
    plt.scatter(visitedX, visitedY,s = area, alpha = 0.3, c=color)
    plt.colorbar()
    plt.show()


    """
    각 지역에 대한 사용자들의 방문 빈도를 산점도로 표현 
    """
    visitedX = []
    visitedY = []
    area = []
    color = []    
    for i in range(len(visitedArea)):
        for j in range(len(visitedArea[0])):
            if(visitedArea[i][j] > 0.0):
                area.append(visitedArea[i][j]/19453 * 500)
                visitedX.append(j)
                visitedY.append(i)
                color.append(visitedArea[i][j]/19453)
    # plt.matshow(visitedArea)
    # plt.colorbar()
    # plt.show()
    # print(max(area))
    plt.scatter(visitedX, visitedY,s = area, alpha = 0.3, c=color)
    plt.colorbar()
    plt.show()

def userVisitDataPerArea(city:str):
    userVisitData = pickle_load("./Yelp/data/"+city+"userVisitData")

    #각 지역에 사용자가 방문한 횟수
    #vector = np.zeros((425,645,len(userVisitData)))
    vector = dict()
    for i in range(425):
        vector[i] = dict()
        for j in range(645):
            vector[i][j] = dict()
    index2Id = dict()
    id2Index = dict()
    lengthCheckDict = dict()
    idx = 0
    for k,v in userVisitData.items():
        ll = len(v)
        if ll < 1:
            continue
        index2Id[idx] = k
        id2Index[k] = idx
        
        for i in v:
            try:
                vector[i[0]][i[1]][idx] += 1/ll
            except KeyError:
                vector[i[0]][i[1]][idx] = 1/ll
            lengthCheckDict[str(i[0])+","+str(i[1])]=1
        idx+=1
    print("user Visited Area Num : "+ str(len(lengthCheckDict)))
    pickle_save(index2Id,"./Yelp/data/user_index2Id.pkl")
    pickle_save(id2Index,"./Yelp/data/user_id2Index.pkl")
    pickle_save(vector,"./Yelp/data/userVisitDataPerArea.pkl")

def visitedCategoryPerArea(city:str):
    areaCategory = pickle_load("./Yelp/data/"+city+"areaCategory")
    
    index2Cate = dict()
    cate2Index = dict()
    idx = 0

    for colNum,row in areaCategory.items():
        for rowNum,area in row.items():
            for i in area:
                try:
                    temp = cate2Index[i]
                except KeyError:
                    cate2Index[i] = idx
                    index2Cate[idx] = i
                    idx+=1

    #vector = np.zeros((425,645,len(cate2Index)))
    vector = dict()
    for i in range(425):
        vector[i] = dict()
        for j in range(645):
            vector[i][j] = dict()
    lengthCheckDict = dict()
    for i in range(425):
        for j in range(645):
            for k, v in areaCategory[i][j].items():
                try:
                    vector[i][j][cate2Index[k]] += v
                except KeyError:
                    vector[i][j][cate2Index[k]] = v
                lengthCheckDict[str(i)+","+str(j)]=1

    print("category Area Num : "+ str(len(lengthCheckDict)))
    pickle_save(index2Cate,"./Yelp/data/index2Cate.pkl")
    pickle_save(cate2Index,"./Yelp/data/cate2Index.pkl")
    pickle_save(vector,"./Yelp/data/visitedCategoryPerArea.pkl")

        
if __name__ == "__main__":
    #getArea("39.86492399 -75.651673 _ 40.247267 -74.8937988281","39.86492399 -75.651673 _ 40.247267 -74.8937988281Review",100, minVisitedNum=10)

    userVisitDataPerArea("philadelphia10_")
    print("1")
    visitedCategoryPerArea("philadelphia10_")
    #drawInGraphs("39.86492399 -75.651673 _ 40.247267 -74.8937988281")
    #getArea("39.86492399 -75.651673 _ 40.247267 -74.8937988281","39.86492399 -75.651673 _ 40.247267 -74.8937988281Review",100, minVisitedNum=10)
    #mergeData()