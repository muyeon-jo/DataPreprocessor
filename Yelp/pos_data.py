import pandas as pd
import numpy as np
from haversine import haversine
import math
import multiprocessing as mp
def getAreaWithPos():
    """
    가게 데이터를 이용하여 전체 지역을 모두 포함하는 크기의 직사각형을 계산한다.
    반지름은 약 6371km 로 계산되며 위도에따라 같은 경도에 대한 길이가 달라지기 때문에
    이를 마름모꼴이 아닌 직사각형으로 변환시키기 위해 변화 비율을 곱해줌으로써
    높은 위도에서의 직사각형의 길이를 늘려준다.
    """
    
    #reviewData = pd.read_csv("./Yelp/data/Philadelphia_review.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/Philadelphia_business.csv", engine='python', encoding = "ISO-8859-1",names =['business_id','latitude', 'longitude','categories'], header = None)
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
    heigt1 = haversine((latitude_max,midPoint+x),(latitude_min,midPoint+x), unit="m")
    heigt2 = haversine((latitude_max,midPoint-x),(latitude_min,midPoint-x), unit="m")
    print(heigt1)
    print(heigt2)
    
if __name__ == "__main__":
    getAreaWithPos()