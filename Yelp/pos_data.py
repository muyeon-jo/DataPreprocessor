import pandas as pd
import numpy as np
from haversine import haversine
import multiprocessing as mp
def getAreaWithPos():
    """
    가게 데이터를 이용하여 전체 지역을 모두 포함하는 크기의 사각형을 계산한다.
    
    """
    #reviewData = pd.read_csv("./Yelp/data/Philadelphia_review.csv", engine='python', encoding = "ISO-8859-1",names =['user_id', 'business_id','text'], header = None)
    businessData = pd.read_csv("./Yelp/data/Philadelphia_business.csv", engine='python', encoding = "ISO-8859-1",names =['business_id','latitude', 'longitude','categories'], header = None)
    latitude_max = businessData['latitude'].max()
    latitude_min = businessData['latitude'].min()
    print(latitude_max)
    print(latitude_min)
    longitude_max = businessData['longitude'].max()
    longitude_min = businessData['longitude'].min()
    print(longitude_max)
    print(longitude_min)
    width1 = haversine((latitude_max,longitude_max),(latitude_max,longitude_min), unit="m")
    width2 = haversine((latitude_min,longitude_max),(latitude_min,longitude_min), unit="m")
    print(width1)
    print(width2)
    heigt1 = haversine((latitude_max,longitude_max),(latitude_min,longitude_max), unit="m")
    heigt2 = haversine((latitude_max,longitude_min),(latitude_min,longitude_min), unit="m")
    print(heigt1)
    print(heigt2)
if __name__ == "__main__":
    getAreaWithPos()