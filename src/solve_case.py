import logging

from src.data.extract_transform_load import extract_transform_load
from src.data.data_analysis import data_analysis

logging.basicConfig(level=logging.INFO)

def solve_case(show_plots = False):
    
    logging.info("Beginning ETL...")

    # ETL
    airbnb_listings, \
    encoded_columns, \
    least_available_airbnb_listings, \
    least_available_airbnb_apartments, \
    top_booked_airbnb_listings, \
    top_booked_airbnb_apartments, \
    airbnb_listings_geohash_p6, \
    airbnb_listings_geohash_p7, \
    top_booked_airbnb_apartments_geohash_p6, \
    top_booked_airbnb_apartments_geohash_p7, \
    vivareal_listings, \
    vivareal_lots_selling, \
    vivareal_apartments_selling, \
    vivareal_lots_nbh, \
    vivareal_apartments_nbh \
                            = extract_transform_load()

    logging.info("Beginning Data Analysis...")                        
            
    # Data analysis, plot and save figures
    data_analysis(  airbnb_listings, 
                    encoded_columns, 
                    least_available_airbnb_listings,
                    least_available_airbnb_apartments,
                    top_booked_airbnb_listings,
                    top_booked_airbnb_apartments,
                    airbnb_listings_geohash_p6,
                    airbnb_listings_geohash_p7,
                    top_booked_airbnb_apartments_geohash_p6,
                    top_booked_airbnb_apartments_geohash_p7,
                    vivareal_listings,
                    vivareal_lots_selling,
                    vivareal_apartments_selling,
                    vivareal_lots_nbh,
                    vivareal_apartments_nbh,
                    show_plots = show_plots)

    logging.info("Completed")
            
            
if __name__ == '__main__':
    solve_case()
