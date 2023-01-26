import pandas as pd
import numpy as np
from haversine import haversine
import math
import multiprocessing as mp
import pickle
import matplotlib.pyplot as plt
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

def getAreaWithPos(business:str, review:str, size):
    """
    가게 데이터를 이용하여 전체 지역을 모두 포함하는 크기의 직사각형을 계산한다.
    반지름은 약 6371km 로 계산되며 위도에따라 같은 경도에 대한 길이가 달라지기 때문에
    이를 마름모꼴이 아닌 직사각형으로 변환시키기 위해 변화 비율을 곱해줌으로써
    높은 위도에서의 직사각형의 길이를 늘려준다.
    """
    
    #reviewData = pd.read_csv("./Yelp/data/"+review+".csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/"+business+".csv", engine='python', encoding = "ISO-8859-1",names =['business_id','latitude', 'longitude','categories'], header = None)
    latitude_max = businessData['latitude'].max()
    latitude_min = businessData['latitude'].min()
    longitude_max = businessData['longitude'].max()
    longitude_min = businessData['longitude'].min()
    print("lat_max:{latmax} lat_min:{latmin}".format(latmax=latitude_max, latmin=latitude_min))
    print("lng_max:{lngmax} lng_min:{lngmin}".format(lngmax=longitude_max, lngmin=longitude_min))

    width1 = haversine((latitude_max,longitude_max),(latitude_max,longitude_min), unit="m")
    width2 = haversine((latitude_min,longitude_max),(latitude_min,longitude_min), unit="m")
    

    RADIUS = haversine((0,0),(0,180),unit='m') / math.pi
    angle1 = width1/RADIUS*180
    angle2 = width2/RADIUS*180
    posDiff = longitude_max - longitude_min
    angleRatio = angle2/angle1
    x = posDiff*angleRatio/2
    midPoint = (longitude_max+longitude_min)/2
    print("angle1:{a1} angle2:{a2}".format(a1=angle1, a2=angle2))

    width1 = haversine((latitude_max,midPoint+x),(latitude_max,midPoint-x), unit="m")
    print(width1)
    print(width2)
    height1 = haversine((latitude_max,midPoint+x),(latitude_min,midPoint+x), unit="m")
    height2 = haversine((latitude_max,midPoint-x),(latitude_min,midPoint-x), unit="m")
    print(height1)
    print(height2)

    colnum = int(width2/size)
    rownum = int(height1/size)
    delta = (midPoint+x)/rownum
    for i in rownum:
        lat_min = latitude_min + (latitude_max - latitude_min)/rownum*(i)
        lat_max = latitude_min + (latitude_max - latitude_min)/rownum*(i+1)
        for j in colnum:
            lng_min = longitude_min
            
def getArea(business:str, review:str, size):
    """
    """
    reviewData = pd.read_csv("./Yelp/data/"+review+".csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/"+business+".csv", engine='python', encoding = "ISO-8859-1")
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
    print(height1)
    print(height2)

    colnum = int((width2+width1)/2/size)
    rownum = int(height1/size)
    delta = (longitude_max - longitude_min)/colnum
    areaRangeArr = np.zeros((rownum, colnum, 4))
    areaBusinessArr = []
    businessPos = dict()
    for i in range(rownum):
        lat_min = latitude_min + (latitude_max - latitude_min)/rownum*(i)
        lat_max = latitude_min + (latitude_max - latitude_min)/rownum*(i+1)
        line = []
        for j in range(colnum):
            point = []
            lng_min = longitude_min + delta * j
            lng_max = longitude_min + delta * (j+1)

            areaRangeArr[i][j][0] = lat_min
            areaRangeArr[i][j][1] = lat_max
            areaRangeArr[i][j][2] = lng_min
            areaRangeArr[i][j][3] = lng_max

            temp = businessData[businessData['latitude']>=lat_min]
            temp = temp[temp['longitude']>=lng_min]
            if j == colnum-1:
                temp = temp[temp['longitude']<=lng_max]
            else:
                temp = temp[temp['longitude']<lng_max]

            if i == rownum-1:
                temp = temp[temp['latitude']<=lat_max]
            else:
                temp = temp[temp['latitude']<lat_max]

            for index, data in temp.iterrows():
                businessPos[data['business_id']] = (i,j)
                point.append([data['business_id'],data['categories']])
            line.append(point)
        areaBusinessArr.append(line)
    # businessPos = pickle_load("./Yelp/data/"+business+" businessPos")
    # areaRangeArr = pickle_load("./Yelp/data/"+business+" Area range")
    # areaBusinessArr = pickle_load("./Yelp/data/"+business+" area list")
    userVisitData = dict()
    areaCategory = dict()
    visitedArea = np.zeros((rownum,colnum))
    for i in range(rownum):
        areaCategory[i]=dict()
        for j in range(colnum):
            areaCategory[i][j] = dict()
    for index, data in reviewData.iterrows():
        try:
            userVisitData[data['user_id']].append(businessPos[data['business_id']])
        except KeyError:
            li = [businessPos[data['business_id']]]
            userVisitData[data['user_id']] = li

        

        pos = businessPos[data['business_id']]
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

    pickle_save(areaRangeArr,"./Yelp/data/"+business+" Area range")
    pickle_save(areaBusinessArr,"./Yelp/data/"+business+" area list")
    pickle_save(businessPos,"./Yelp/data/"+business+" businessPos")
    pickle_save(userVisitData,"./Yelp/data/"+business+" userVisitData")
    pickle_save(areaCategory,"./Yelp/data/"+business+" areaCategory")
    pickle_save(visitedArea,"./Yelp/data/"+business+" visitedArea")
    plt.matshow(visitedArea)
    plt.colorbar()
    plt.show()
if __name__ == "__main__":
    getArea("39.86492399 -75.651673 _ 40.247267 -74.8937988281","39.86492399 -75.651673 _ 40.247267 -74.8937988281Review",1000)