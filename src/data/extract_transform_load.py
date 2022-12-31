from src.data.methods.etl_methods import *

def extract_transform_load():
    
    # Airbnb
    # Get locations of all airbnb listings
    airbnb_locations = get_airbnb_locations()
    
    # Get all characteristics of all airbnb listings
    airbnb_listings, encoded_columns = get_airbnb_listings()
    
    # Use the correct lat/lon
    airbnb_listings = replace_lat_lon(airbnb_listings, airbnb_locations)
    
    airbnb_listings = include_geohash(airbnb_listings, 6)
    airbnb_listings = include_geohash(airbnb_listings, 7)
    
    # Get price and availability of all airbnb listings
    airbnb_price_av = get_airbnb_price_av()
    
    airbnb_listings_av_rate = calculate_availability_rate(airbnb_price_av)
    
    # Include availability rate into all airbnb_listings
    airbnb_listings = \
        include_availability(airbnb_listings, airbnb_listings_av_rate)
    
    # Calculate average price of each airbnb listing
    avg_func = dd_mode()
    airbnb_listings_mode_price = calculate_avg_price(airbnb_price_av, avg_func)
    
    # Include average price into all airbnb_listings
    airbnb_listings = \
        include_avg_price(airbnb_listings, airbnb_listings_mode_price)
    
    # Select least availables entries among all airbnb listings
    least_available_airbnb_listings = \
        get_least_available_airbnb_listings(airbnb_listings)
    
    # Select least availables apartments
    least_available_airbnb_apartments = \
        get_apartments(least_available_airbnb_listings)
    
    # Select top booked entries among all airbnb listings
    top_booked_airbnb_listings = \
        get_top_booked_airbnb_listings(airbnb_listings, min_reviews = 50)
    
    # Select top booked apartments
    top_booked_airbnb_apartments = get_apartments(top_booked_airbnb_listings)
    
    # Group all airbnb listings by geohash using a given precision
    airbnb_listings_geohash_p6 = groupby_geohash(airbnb_listings, precision = 6)
    airbnb_listings_geohash_p7 = groupby_geohash(airbnb_listings, precision = 7)

    # Group top booked airbnb apartments by geohash using a given precision
    top_booked_airbnb_apartments_geohash_p6 = \
        groupby_geohash(top_booked_airbnb_apartments, precision = 6)
    top_booked_airbnb_apartments_geohash_p7 = \
        groupby_geohash(top_booked_airbnb_apartments, precision = 7)
    
    
    # Vivareal
    # Get  all vivareal listings
    vivareal_listings = get_vivareal_listings()
    
    # Select only lots
    vivareal_lots_selling = get_lots_selling(vivareal_listings)
    
    # Select only apartments
    vivareal_apartments_selling = get_apartments_selling(vivareal_listings)
    
    # Group lots by neighborhood
    vivareal_lots_nbh = groupby_neighborhood(vivareal_lots_selling)
    
    # Group apartments by neighborhood
    vivareal_apartments_nbh = groupby_neighborhood(vivareal_apartments_selling)
    
    
    return  airbnb_listings, \
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
            vivareal_apartments_nbh
