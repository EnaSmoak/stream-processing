import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree


points = gpd.read_file('hotosm_gha_points_of_interest_points_shp/hotosm_gha_points_of_interest_points.shp')

def ckdnearest(gdA, gdB, k):
    """Quickly fine the k nearest points close to another
    
    :param gdA: geopandas dataframe A
    :param gdB: geopandas dataframe B
    :param k: number of nearest neighbours to find
    
    :returns: joined geopandas dataframe with distance computed 
    in the last column
    """
    
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)))
    
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k)
    
    dd = []
    
    # when idx is a 1d array
    try:
        n = idx.shape[1]
    except IndexError:
        n = 1

    for i in range(n):
        df = pd.concat(
            [
                gdA, 
                gdB.loc[idx.T[i], gdB.columns != 'geometry'].reset_index(), 
                pd.Series(dist.T[i], name='dist')
            ], 
            axis=1
        )
        dd.append(df)
    

    gdf = pd.concat([*dd], axis=0).sort_values('id')

    return gdf


def join_to_poi(df, k=1):
    """Join a pandas dataframe to the points of interest dataset.
    
    :param df: input dataframe. Must be a pandas dataframe
    :param k: by default, join to the closest point of interest. 
    Increasing this will join to the number of points specified.
    
    :return: a dataframe where each row reprsents the input joined 
    with the point of interest. When k is >=2, multiple rows for each
    row of the input will be created together with the point of interest
    """
    
    # convert the df into a geopandas df
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lon, df.lat)
    )
    
    # drop all null coordinates else burn!
    gdf = gdf[~gdf.lat.isna()]
    
    return ckdnearest(gdf, points, k)
