# import dependencies
import argparse
import fiona
import pandas as pd
import shapely
from shapely.geometry import asShape, shape, Point
import tqdm
from tqdm import tqdm

if __name__ == '__main__':
    # create argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_path', type=str, help='Path to the csv file needed for processing')
    parser.add_argument('--out_path', type=str, help='Path to save processed file')
    args = parser.parse_args()

    # parse shapes and define mapping function
    shapes = []
    location_is = []

    with fiona.open('geo_export_971c31ea-d4e3-4009-b2c3-270f511e4a31.shp', 'r') as fiona_collection:
        for shapefile_record in fiona_collection:
            location_i = shapefile_record['properties']['location_i']
            if location_i in [87, 114, 90, 230, 239, 236]:
                location_is.append(location_i)
                shapes.append(asShape(shapefile_record['geometry']))

    def map_(shapes, long_, lat):
        for idx in range(len(shapes)):
            point = Point(long_, lat)
            if shapes[idx].contains(point):
                return location_i[idx]
        return None

    # map coordinates to zones
    df = pd.read_csv(args.path, header=None)
    df[len(df.columns)] = pd.progress_apply(lambda row: map_(shapes, df[7], df[6]))
    df.to_csv(args.out_path)

