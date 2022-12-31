import matplotlib.pyplot as plt

from src.data.methods.data_analysis_methods import *

def data_analysis(  airbnb_listings,
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
                    show_plots = False):
                    
    
    # Select top3 geohash using different parameters to sort
    top3_listings_locations_p6_price = \
        get_top3_locations(airbnb_listings_geohash_p6, 'Mean price')
    top3_listings_locations_p6_reviews = \
        get_top3_locations(airbnb_listings_geohash_p6, 'Sum of reviews')

    top3_listings_locations_p7_price = \
        get_top3_locations(airbnb_listings_geohash_p7, 'Mean price')
    top3_listings_locations_p7_reviews = \
        get_top3_locations(airbnb_listings_geohash_p7, 'Sum of reviews')

    top3_apartment_locations_p6_price = \
        get_top3_locations(top_booked_airbnb_apartments_geohash_p6, 'Mean price')
    top3_apartment_locations_p6_reviews = \
        get_top3_locations(top_booked_airbnb_apartments_geohash_p6, 'Sum of reviews')

    top3_apartment_locations_p7_price = \
        get_top3_locations(top_booked_airbnb_apartments_geohash_p7, 'Mean price')
    top3_apartment_locations_p7_reviews = \
        get_top3_locations(top_booked_airbnb_apartments_geohash_p7, 'Sum of reviews')
    
    # save these or print /\
    
    
    # Setting default configurations for plots
    SMALL_FONT_SIZE = 12
    MEDIUM_FONT_SIZE = 14
    BIGGER_FONT_SIZE = 16
    HUGE_FONT_SIZE = 20

    plt.rc('font', size=SMALL_FONT_SIZE)          # controls default text sizes
    plt.rc('axes', titlesize=BIGGER_FONT_SIZE)     # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_FONT_SIZE)    # fontsize of the x and y labels
    plt.rc('xtick', labelsize=BIGGER_FONT_SIZE)    # fontsize of the tick labels
    plt.rc('ytick', labelsize=MEDIUM_FONT_SIZE)    # fontsize of the tick labels
    plt.rc('legend', fontsize=MEDIUM_FONT_SIZE)    # legend fontsize
    plt.rc('figure', titlesize=BIGGER_FONT_SIZE)  # fontsize of the figure title

    plt.style.use('ggplot')
    
    # Begin plots
    
    # Plot price histogram among all listings on Airbnb
    plot_price_hist(df = airbnb_listings, 
                    col = 'mode_price', 
                    bins = 50,
                    title = 'Price histogram of all Airbnb listings', 
                    xlabel = 'Price (R$)',
                    figsize = (12,10),
                    barlabel_rotation = 90,
                    show = show_plots,
                    save_path = './reports/figures/main/price_histogram_all_airbnb_listings.png',
                    )
    
    # Plot listing types among all listings on Airbnb
    plot_number_of(df = airbnb_listings, 
                   col = 'listing_type', 
                   title = 'Count of listing type among all Airbnb listings', 
                   xlabel = 'Listing type', 
                   figsize = (12,12), 
                   xticks_rotation=90,
                   show = show_plots,
                   save_path = './reports/figures/main/listing_types_all_airbnb_listings.png')
    
    # Plot listing types among top booked listings on Airbnb
    plot_number_of(df = top_booked_airbnb_listings, 
            col = 'listing_type', 
            title = 'Count of listing type among top booked Airbnb listings', 
            xlabel = 'Listing type', 
            figsize = (12,8), 
            xticks_rotation=45,
            show = show_plots,
            save_path = './reports/figures/main/listing_types_top_booked_airbnb_listings.png')
    
    # Plot number of bedrooms among top booked apartments on Airbnb
    plot_number_of(df = top_booked_airbnb_apartments, 
               col = 'number_of_bedrooms', 
               title = 'Count of no. bedrooms on top booked Airbnb apartments', 
               xlabel = 'No. bedrooms',
               figsize = (12,8),
               show = show_plots,
               save_path = './reports/figures/main/n_bedrooms_top_booked_airbnb_apartments.png')
    
    # Plot number of beds among top booked apartments on Airbnb
    plot_number_of(df = top_booked_airbnb_apartments,
               col = 'number_of_beds', 
               title = 'Count of no. beds on top booked Airbnb apartments', 
               xlabel = 'No. beds',
               figsize = (12,10),
               show = show_plots,
               save_path = './reports/figures/main/n_beds_top_booked_airbnb_apartments.png')
    
    # Plot number of bathrooms among top booked apartments on Airbnb
    plot_number_of(df = top_booked_airbnb_apartments,
               col = 'number_of_bathrooms', 
               title = 'Count of no. bathrooms on top booked Airbnb apartments',
               xlabel = 'No. bathrooms',
               figsize = (12,10),
               show  = show_plots,
               save_path = './reports/figures/main/n_bathrooms_top_booked_airbnb_apartments.png')
    
    # Plot maximum number of guests allowed among top booked apartments on Airbnb
    plot_number_of(df = top_booked_airbnb_apartments,
               col = 'number_of_guests', 
               title = 'Count of maximum no. guests allowed on top booked Airbnb apartments',
               xlabel = 'No. guests',
               figsize = (12,10),
               show = show_plots,
               save_path = './reports/figures/main/n_guests_top_booked_airbnb_apartments.png')
    
    # Plot price histogram among top booked apartments on Airbnb
    plot_price_hist(df = top_booked_airbnb_apartments, 
               col = 'mode_price', 
               bins = 10,
               title = 'Price histogram of top booked Airbnb apartments', 
               xlabel = 'Price (R$)',
               figsize = (12,10),
               barlabel_rotation=0,
               show = show_plots,
               save_path = './reports/figures/main/price_histogram_top_booked_airbnb_apartments.png')
    
    # Plot count of lots selling per neighborhood on VivaReal
    plot_number_of(df = vivareal_lots_selling, 
                   col = 'address_neighborhood', 
                   title = 'Count of lots selling on VivaReal per neighborhood', 
                   xlabel = 'Neighborhood', 
                   figsize = (12,12), 
                   xticks_rotation=90,
                   show = show_plots,
                   save_path = './reports/figures/main/lots_selling_per_neighborhood_vivareal.png')
    
    # Plot count of apartments selling per neighborhood on VivaReal
    plot_number_of(df = vivareal_apartments_selling, 
                   col = 'address_neighborhood', 
                   title = 'Count of apartments selling on VivaReal per neighborhood', 
                   xlabel = 'Neighborhood', 
                   figsize = (12,12), 
                   xticks_rotation=90,
                   show = show_plots,
                   save_path = './reports/figures/main/apartments_selling_per_neighborhood_vivareal.png')

    
    # Plot price/sqm among Vivareal lots being sold
    bar_plot(df = vivareal_lots_nbh,
        x = 'neighborhood',
        y = 'Mean price/Total sqm',
        title = 'Mean price/sqm per neighborhood of lots being sold on VivaReal',
        xlabel = 'Neighborhoods',
        ylabel = 'Price/m²',
        figsize = (12,8),
        xticks_rotation = 45,
        barlabel_rotation = 45,
        show = show_plots,
        save_path = './reports/figures/main/price_sqm_vivareal_lots_selling.png')
    
    # Plot price/sqm among Vivareal apartments being sold
    bar_plot(df = vivareal_apartments_nbh,
        x = 'neighborhood',
        y = 'Mean price/Usable sqm',
        title = 'Mean price/sqm per neighborhood of apartments being sold on VivaReal',
        xlabel = 'Neighborhoods',
        ylabel = 'Price/m²',
        figsize = (12,8),
        xticks_rotation = 45,
        barlabel_rotation = 45,
        show = show_plots,
        save_path = './reports/figures/main/price_sqm_vivareal_apartments_selling.png')

        
    # Plot all encoded columns count among top booked apartments on Airbnb
    plot_encoded_columns(top_booked_airbnb_apartments, 
                         encoded_columns,
                         show = show_plots,
                         save_path = './reports/figures/one_hot_encoding/')
    
    
    
    # MAP PLOTS
    # Create a map analysis of all airbnb listings with respect to mean price
    create_map_analysis(airbnb_listings_geohash_p6, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.2, 
                        color_based_on = 'Mean price', 
                        legend = 'Average price (R$)',
                        save_path = './reports/figures/maps/mean_price_all_airbnb_listings_p6.png')
    
    # Create map analysus of all airbnb listings
    # Create a map analysis of all airbnb listings with respect to mean price
    create_map_analysis(airbnb_listings_geohash_p7, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.2, 
                        color_based_on = 'Mean price', 
                        legend = 'Average price (R$)',
                        save_path = './reports/figures/maps/mean_price_all_airbnb_listings_p7.png')
    
    # Create a map analysis of all airbnb listings with respect to sum of reviews
    create_map_analysis(airbnb_listings_geohash_p6, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.2, 
                        color_based_on = 'Sum of reviews',
                        legend = 'Sum of reviews',
                        save_path = './reports/figures/maps/sum_reviews_all_airbnb_listings_p6.png')
    
    # Create a map analysis of all airbnb listings with respect to sum of reviews
    create_map_analysis(airbnb_listings_geohash_p7, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.2, 
                        color_based_on = 'Sum of reviews',
                        legend = 'Sum of reviews',
                        save_path = './reports/figures/maps/sum_reviews_all_airbnb_listings_p7.png')
    
    # Create map analysis of top booked airbnb apartments
    # Create a map analysis of top booked airbnb apartments with respect to mean price
    create_map_analysis(top_booked_airbnb_apartments_geohash_p6, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.5, 
                        color_based_on = 'Mean price', 
                        legend = 'Average price (R$)',
                        save_path = './reports/figures/maps/mean_price_top_booked_airbnb_apartments_p6.png')
    
    # Create a map analysis of top booked airbnb apartments with respect to mean price
    create_map_analysis(top_booked_airbnb_apartments_geohash_p7, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.5, 
                        color_based_on = 'Mean price', 
                        legend = 'Average price (R$)',
                        save_path = './reports/figures/maps/mean_price_top_booked_airbnb_apartments_p7.png')
    
    # Create a map analysis of top booked airbnb apartments with respect to sum of reviews
    create_map_analysis(top_booked_airbnb_apartments_geohash_p6, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.5, 
                        color_based_on = 'Sum of reviews',
                        legend = 'Sum of reviews',
                        save_path = './reports/figures/maps/sum_reviews_top_booked_airbnb_apartments_p6.png')
    
    # Create a map analysis of top booked airbnb apartments with respect to sum of reviews
    create_map_analysis(top_booked_airbnb_apartments_geohash_p7, 
                        center = (-27.107665, -48.591679),  # Itapema/Morretes 
                        zoom_start = 12.5, 
                        color_based_on = 'Sum of reviews',
                        legend = 'Sum of reviews',
                        save_path = './reports/figures/maps/sum_reviews_top_booked_airbnb_apartments_p7.png')